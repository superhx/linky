/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.superhx.linky.broker.persistence;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import io.grpc.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.Record;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.superhx.linky.broker.persistence.Constants.*;

public class LocalPartitionImpl implements Partition {
  private static final int TIMER_WINDOW = (int) TimeUnit.DAYS.toSeconds(1);
  private static final int TIMER_WHEEL_SEGMENT = (int) TimeUnit.MINUTES.toSeconds(5);

  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private final String partitionName;
  private PartitionMeta meta;
  private AtomicReference<PartitionStatus> status = new AtomicReference<>(PartitionStatus.NOOP);
  private NavigableMap<Integer, Segment> segments;
  private volatile Segment lastSegment;
  private LocalSegmentManager localSegmentManager;
  private CompletableFuture<Segment> lastSegmentFuture;
  private Map<Segment, CompletableFuture<Segment>> nextSegmentFutures = new ConcurrentHashMap<>();
  private ReentrantLock appendLock = new ReentrantLock();
  private Queue<TimerIndex> timerIndexQueue = new LinkedBlockingQueue<>();
  private volatile boolean timerEnable = false;

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
    partitionName = meta.getTopicId() + "@" + meta.getPartition();
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    TimerIndex timerIndex = new TimerIndex().setTimestamp(batchRecord.getVisibleTimestamp());
    if (log.isDebugEnabled()) {
      log.debug("[PARTITION_APPEND]{}", TextFormat.shortDebugString(batchRecord));
    }
    return getLastSegment()
        .thenCompose(
            s -> {
              switch (s.getStatus()) {
                case WRITABLE:
                  return append0(s, batchRecord);
                case REPLICA_BREAK:
                  return nextSegment(s).thenCompose(n -> append0(n, batchRecord));
                case SEALED:
                  return CompletableFuture.completedFuture(
                      new Segment.AppendResult(Segment.AppendResult.Status.SEALED));
              }
              throw new LinkyIOException(String.format("Unknown status %s", s.getStatus()));
            })
        .thenApply(
            appendResult -> {
              switch (appendResult.getStatus()) {
                case SUCCESS:
                  timerIndex.setOffset(appendResult.getOffset()).setIndex(appendResult.getIndex());
                  timerIndexQueue.offer(timerIndex);
                  ByteBuffer cursor = ByteBuffer.allocate(4 + 8);
                  cursor.putInt(appendResult.getIndex());
                  cursor.putLong(appendResult.getOffset());
                  timerCommitCursor = cursor.array();
                  return new AppendResult(cursor.array());
                default:
                  return new AppendResult(AppendStatus.FAIL);
              }
            });
  }

  private byte[] getMeta(byte[] key) {
    try {
      GetKVResult getKVResult = getKV(key, true).get();
      return Optional.ofNullable(getKVResult)
          .map(rst -> rst.getBatchRecord())
          .map(r -> r.getRecords(0).getValue().toByteArray())
          .orElse(null);
    } catch (Exception e) {
      e.printStackTrace();
      throw new LinkyIOException(e);
    }
  }

  private boolean matchExpectedTimerIndex(TimerIndex timerIndex) {
    // TODO: move when msg expired
    for (; ; ) {
      if (timerIndex.getIndex() == expectedNextCursor.getIndex()
          && timerIndex.getOffset() == expectedNextCursor.getOffset()) {
        return true;
      }
      Segment segment = segments.get(timerIndex.getIndex());
      if (segment.isSealed() && segment.getEndOffset() == expectedNextCursor.getOffset()) {
        expectedNextCursor.setIndex(expectedNextCursor.getIndex() + 1);
        expectedNextCursor.setOffset(0);
      } else {
        return false;
      }
    }
  }

  private volatile long timerNextTimestamp = -1L;
  private volatile long savedTimerNextTimestamp = -1L;

  private void loadTimer() {
    byte[] timerTriggerTimestampBytes = getMeta(TIMER_NEXT_TIMESTAMP_KEY);
    if (timerTriggerTimestampBytes == null) {
      timerNextTimestamp = System.currentTimeMillis() / 1000 * 1000;
      log.info("[TIMER_NEXT_TIMESTAMP_INIT]{}", timerNextTimestamp);
      timerTriggerTimestampBytes = Utils.getBytes(timerNextTimestamp);
      saveTimerNextTimestamp();
    }
    timerNextTimestamp = ByteBuffer.wrap(timerTriggerTimestampBytes).getLong();
    log.info("[TIMER_NEXT_TIMESTAMP_LOAD]{}", timerNextTimestamp);
    savedTimerNextTimestamp = timerNextTimestamp;

    byte[] timerCommitCursor = getMeta(TIMER_SLO_KEY);
    if (timerCommitCursor == null) {
      timerCommitCursor = new byte[12];
      this.timerCommitCursor = timerCommitCursor;
    } else {
      savedTimerCommitCursor = timerCommitCursor;
    }
    this.timerCommitCursor = timerCommitCursor;
    log.info("[TIMER_COMMIT_CURSOR_LOAD]{}", Cursor.get(timerCommitCursor));
  }

  private void saveTimerNextTimestamp() {
    if (savedTimerNextTimestamp == timerNextTimestamp) {
      return;
    }
    append(
        BatchRecord.newBuilder()
            .setFlag(INVISIBLE_FLAG | META_FLAG)
            .addRecords(
                Record.newBuilder()
                    .setKey(ByteString.copyFrom(TIMER_NEXT_TIMESTAMP_KEY))
                    .setValue(ByteString.copyFrom(Utils.getBytes(timerNextTimestamp)))
                    .build())
            .build());
    savedTimerNextTimestamp = timerNextTimestamp;
    log.info("[TIMER_NEXT_TIMESTAMP_SAVE]{}", savedTimerNextTimestamp);
  }

  private void saveTimerCommitCursor() {
    if (timerCommitCursor.equals(savedTimerCommitCursor)) {
      return;
    }
    append(
        BatchRecord.newBuilder()
            .setFlag(INVISIBLE_FLAG | META_FLAG)
            .addRecords(
                Record.newBuilder()
                    .setKey(ByteString.copyFrom(TIMER_SLO_KEY))
                    .setValue(ByteString.copyFrom(timerCommitCursor))
                    .build())
            .build());
    savedTimerCommitCursor = timerCommitCursor;
    log.info("[TIMER_COMMIT_CURSOR_SAVE]{}", Cursor.get(savedTimerCommitCursor));
  }

  protected void buildTimerIndex() throws Exception {
    Cursor expectedNextCursor = this.expectedNextCursor;
    Map<Integer, List<TimerIndex>> timestamp2Index = new HashMap<>();
    for (TimerIndex timerIndex = timerIndexQueue.poll();
        timerIndex != null;
        timerIndex = timerIndexQueue.poll()) {
      //      if (expectedNextCursor == null) {
      //        this.expectedNextCursor = Cursor.get(getMeta(Constants.TIMER_SLO_KEY));
      //        expectedNextCursor = this.expectedNextCursor;
      //      }
      //      if (!matchExpectedTimerIndex(timerIndex)) {
      //        if (log.isDebugEnabled()) {
      //          log.debug("skip {} expected {}", timerIndex, expectedNextCursor);
      //        }
      //        continue;
      //      }
      //      expectedNextCursor.setOffset(expectedNextCursor.getOffset() + 1);

      if (timerIndex.getTimestamp() == 0) {
        continue;
      }

      int slot = (int) (timerIndex.getTimestamp() / 1000 % TIMER_WINDOW);
      // index 是否要存储真实时间，滚动的时候不需要重新读取原始的时间
      List<TimerIndex> indexes = timestamp2Index.get(slot);
      if (indexes == null) {
        indexes = new LinkedList<>();
        timestamp2Index.put(slot, indexes);
      }
      indexes.add(timerIndex);
    }
    if (timestamp2Index.size() == 0) {
      return;
    }

    Segment lastSegment = getLastSegment().get();
    // index offset timestamp
    for (Map.Entry<Integer, List<TimerIndex>> entry : timestamp2Index.entrySet()) {
      int slot = entry.getKey();
      List<TimerIndex> indexes = entry.getValue();
      byte[] indexesBytes = TimerIndex.toBytes(indexes);
      BatchRecord.Builder indexesRecord = BatchRecord.newBuilder();
      indexesRecord
          .setFlag(INVISIBLE_FLAG | TIMER_INDEX_FLAG)
          .addRecords(
              Record.newBuilder()
                  .putHeaders(TIMER_SLOT_RECORD_HEADER, ByteString.copyFrom(Utils.getBytes(slot)))
                  .putHeaders(
                      TIMER_PRE_CURSOR_HEADER, ByteString.copyFrom(getInflightTimerCursor(slot)))
                  .setValue(ByteString.copyFrom(indexesBytes))
                  .build());
      // TODO: handle slo & recover

      appendLock.lock();
      try {
        ByteBuffer cursor = ByteBuffer.allocate(4 + 8);
        Segment.AppendContext context =
            new Segment.AppendContext()
                .setHook(
                    new Segment.AppendHook() {
                      @Override
                      public void before(
                          Segment.AppendContext context, BatchRecord.Builder record) {
                        cursor.putInt(context.getIndex());
                        cursor.putLong(context.getOffset());
                        putInflightTimerCursor(slot, cursor.array());
                      }
                    });
        lastSegment
            .append(context, indexesRecord.build())
            .thenAccept(
                appendResult -> {
                  switch (appendResult.getStatus()) {
                    case SUCCESS:
                      putCommitTimerCursor(slot, cursor.array());
                      break;
                    default:
                      throw new RuntimeException(
                          String.format("append fail %s", appendResult.getStatus()));
                  }
                });
      } finally {
        appendLock.unlock();
      }
    }
  }

  private volatile Cursor expectedNextCursor;
  private Map<Integer, CursorSegment> cursorSegments = new ConcurrentHashMap<>();
  private byte[] savedTimerCommitCursor;
  private byte[] timerCommitCursor;
  private static final ScheduledExecutorService timer = Utils.newScheduledThreadPool(1, "Timer");
  private ScheduledFuture timerIndexBuilder;
  private ScheduledFuture timerSnapshot;

  private void startTimerService() {
    timerIndexBuilder =
        timer.scheduleWithFixedDelay(
            () -> {
              try {
                buildTimerIndex();
                visibleTimer();
              } catch (Throwable e) {
                e.printStackTrace();
              }
            },
            10,
            10,
            TimeUnit.MILLISECONDS);
    timerSnapshot =
        timer.scheduleWithFixedDelay(
            () -> {
              try {
                //                timerSnapshot();
              } catch (Throwable e) {
                e.printStackTrace();
              }
            },
            1,
            1,
            TimeUnit.MINUTES);
  }

  private void visibleTimer() {
    long now = System.currentTimeMillis();
    if (now - timerNextTimestamp < 1000L) {
      return;
    }
    for (long timestamp = timerNextTimestamp; timestamp <= now / 1000 * 1000; timestamp += 1000) {
      CursorSegment cursorSegment = getTimerCursor((int) (timestamp / 1000 % TIMER_WINDOW));
      cursorSegment.trigger(timestamp);
    }
    timerNextTimestamp = now / 1000 * 1000;
  }

  private void timerSnapshot() {
    for (CursorSegment segment : cursorSegments.values()) {
      segment.save();
    }
    saveTimerNextTimestamp();
    saveTimerCommitCursor();
  }

  private static byte[] getTimerSlotSegmentKey(int slotSegment) {
    return ByteBuffer.allocate(TIMER_SLOT_SEGMENT_KEY_PREFIX.length + 4)
        .put(TIMER_SLOT_SEGMENT_KEY_PREFIX)
        .putInt(slotSegment)
        .array();
  }

  private CursorSegment getTimerCursor(int slot) {
    int slotSegment = slot / TIMER_WHEEL_SEGMENT;
    CursorSegment cursorSegment = cursorSegments.get(slotSegment);
    try {
      if (cursorSegment == null) {
        // 避免没有timer的也初始化
        cursorSegment =
            new CursorSegment(slotSegment, getMeta(getTimerSlotSegmentKey(slotSegment)));
        cursorSegments.put(slotSegment, cursorSegment);
      }
      return cursorSegment;
    } catch (Exception e) {
      e.printStackTrace();
      throw new LinkyIOException(e);
    }
  }

  private byte[] getInflightTimerCursor(int slot) {
    CursorSegment cursorSegment = getTimerCursor(slot);
    byte[] cursor = cursorSegment.getInflightCursor(slot);
    if (log.isDebugEnabled()) {
      ByteBuffer buf = ByteBuffer.wrap(cursor);
      log.debug("inflight cursor: {}-{}", buf.getInt(), buf.getLong());
    }
    return cursor;
  }

  private void putInflightTimerCursor(int slot, byte[] cursor) {
    int slotSegment = slot / TIMER_WHEEL_SEGMENT;
    CursorSegment cursorSegment = cursorSegments.get(slotSegment);
    cursorSegment.putInflightCursor(slot, cursor);
  }

  private void putCommitTimerCursor(int slot, byte[] cursor) {
    int slotSegment = slot / TIMER_WHEEL_SEGMENT;
    CursorSegment cursorSegment = cursorSegments.get(slotSegment);
    cursorSegment.putCommitCursor(slot, cursor);
  }

  public CompletableFuture<Segment.AppendResult> append0(Segment segment, BatchRecord record) {
    appendLock.lock();
    try {
      Segment.AppendContext context = new Segment.AppendContext();
      return segment.append(context, record);
    } finally {
      appendLock.unlock();
    }
  }

  protected CompletableFuture<Segment> getLastSegment() {
    if (lastSegmentFuture != null) {
      return lastSegmentFuture;
    }
    synchronized (this) {
      if (lastSegmentFuture != null) {
        return lastSegmentFuture;
      }

      CompletableFuture<Segment> lastSegmentFuture = new CompletableFuture();
      localSegmentManager
          .nextSegment(
              meta.getTopicId(),
              meta.getPartition(),
              segments.isEmpty() ? Segment.NO_INDEX : segments.lastEntry().getValue().getIndex())
          .thenAccept(
              s -> {
                lastSegmentFuture.complete(s);
                this.lastSegment = s;
                segments.put(s.getIndex(), s);
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                lastSegmentFuture.completeExceptionally(t);
                return null;
              });
      this.lastSegmentFuture = lastSegmentFuture;
      return lastSegmentFuture;
    }
  }

  protected CompletableFuture<Segment> nextSegment(Segment lastSegment) {
    CompletableFuture<Segment> future = nextSegmentFutures.get(lastSegment);
    if (future != null) {
      return future;
    }
    synchronized (this) {
      future = nextSegmentFutures.get(lastSegment);
      if (future != null) {
        return future;
      }
      future = new CompletableFuture<>();
      nextSegmentFutures.put(lastSegment, future);
      lastSegment
          .seal()
          .thenCompose(
              n ->
                  localSegmentManager.nextSegment(
                      meta.getTopicId(), meta.getPartition(), lastSegment.getIndex()))
          .thenAccept(
              s -> {
                this.lastSegment = s;
                segments.put(s.getIndex(), s);
                nextSegmentFutures.get(lastSegment).complete(s);
                lastSegmentFuture = nextSegmentFutures.get(lastSegment);
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                nextSegmentFutures.remove(lastSegment);
                return null;
              });
      return future;
    }
  }

  @Override
  public CompletableFuture<GetResult> get(byte[] cursor) {
    ByteBuffer cursorBuf = ByteBuffer.wrap(cursor);
    int segmentIndex = cursorBuf.getInt();
    long offset = cursorBuf.getLong();

    GetResult getResult = new GetResult();
    CompletableFuture<BatchRecord> recordFuture;
    Segment segment;
    for (; ; ) {
      segment = segments.get(segmentIndex);
      if (segment.isSealed() && segment.getEndOffset() <= offset) {
        if (segments.containsKey(segmentIndex + 1)) {
          segmentIndex++;
          offset = 0;
          continue;
        } else {
          return CompletableFuture.completedFuture(
              getResult.setStatus(GetStatus.NO_NEW_MSG).setNextCursor(cursor));
        }
      } else {
        break;
      }
    }
    if (segment != null) {
      recordFuture = segment.get(offset);
    } else {
      recordFuture = CompletableFuture.completedFuture(null);
    }
    ByteBuffer nextCursor = ByteBuffer.allocate(4 + 8);
    nextCursor.putInt(segmentIndex);
    long finalOffset = offset;
    int finalSegmentIndex = segmentIndex;
    return recordFuture.thenCompose(
        r -> {
          if (r == null) {
            getResult.setStatus(GetStatus.NO_NEW_MSG);
            getResult.setNextCursor(cursor);
            return CompletableFuture.completedFuture(getResult);
          } else {
            if ((r.getFlag() & Constants.LINK_FLAG) != 0) {
              Cursor linkedCursor =
                  Cursor.get(
                      r.getRecords((int) (finalOffset - r.getFirstOffset()))
                          .getValue()
                          .toByteArray());
              // TODO: 不能直接用 get，需要修正 nextoffset
              nextCursor.putLong(finalOffset + r.getRecordsCount());
              return get(linkedCursor.toBytes())
                  .thenApply(linkedGetResult -> linkedGetResult.setNextCursor(nextCursor.array()));
            }

            if ((r.getFlag() & INVISIBLE_FLAG) != 0) {
              return get(new Cursor(finalSegmentIndex, finalOffset + 1).toBytes());
            }

            getResult.setBatchRecord(r);
            nextCursor.putLong(finalOffset + r.getRecordsCount());
            getResult.setNextCursor(nextCursor.array());
            return CompletableFuture.completedFuture(getResult);
          }
        });
  }

  @Override
  public CompletableFuture<GetKVResult> getKV(byte[] key, boolean meta) {
    // TODO: async refactor
    CompletableFuture future = new CompletableFuture();
    BatchRecord record = null;
    try {
      Segment lastSegment = this.lastSegment;
      if (lastSegment != null) {
        record = lastSegment.getKV(key, meta).get();
      }
      if (record != null) {
        future.complete(new GetKVResult(record));
        return future;
      }
      for (Map.Entry<Integer, Segment> entry : segments.descendingMap().entrySet()) {
        Segment segment = entry.getValue();
        record = segment.getKV(key, meta).get();
        if (record != null) {
          future.complete(new GetKVResult(record));
          break;
        }
      }
    } catch (Exception e) {
      future.completeExceptionally(e);
      return future;
    }
    future.complete(new GetKVResult());
    return future;
  }

  @Override
  public CompletableFuture<PartitionStatus> open() {
    try {
      return Context.current().fork().call(() -> open0());
    } catch (Exception e) {
      throw new LinkyIOException(e);
    }
  }

  public CompletableFuture<PartitionStatus> open0() {
    if (!status.compareAndSet(PartitionStatus.NOOP, PartitionStatus.OPENING)) {
      log.warn("cannot open {} status partition {}", status.get(), meta);
      return CompletableFuture.completedFuture(this.status.get());
    }

    log.info("partition {} opening...", TextFormat.shortDebugString(meta));
    CompletableFuture<PartitionStatus> openFuture = new CompletableFuture<>();
    localSegmentManager
        .getSegments(meta.getTopicId(), meta.getPartition())
        .thenCompose(
            s -> {
              segments = new ConcurrentSkipListMap<>();
              s.forEach(seg -> segments.put(seg.getIndex(), seg));
              if (!segments.isEmpty()) {
                return segments.lastEntry().getValue().seal();
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenAccept(
            n -> {
              loadTimer();
              startTimerService();
              this.status.set(PartitionStatus.OPEN);
              log.info("partition {} opened", TextFormat.shortDebugString(meta));
              openFuture.complete(this.status.get());
            })
        .exceptionally(
            t -> {
              log.warn("partition {} open fail", meta, t);
              close()
                  .handle(
                      (r, ct) -> {
                        openFuture.complete(this.status.get());
                        return null;
                      });
              return null;
            });
    return openFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    status.set(PartitionStatus.SHUTTING);

    if (timerIndexBuilder != null) {
      timerIndexBuilder.cancel(false);
    }
    if (timerSnapshot != null) {
      timerSnapshot.cancel(false);
    }
    timerSnapshot();

    log.info("partition {} closing...", meta);
    segments = null;
    if (lastSegment == null) {
      log.info("partition {} closed", meta);
      closeFuture.complete(null);
      return closeFuture;
    }
    lastSegment
        .seal()
        .handle(
            (nil, t) -> {
              if (t != null) {
                log.warn("partition {} close fail", meta, t);
              }
              status.compareAndSet(PartitionStatus.SHUTTING, PartitionStatus.SHUTDOWN);
              log.info("partition {} closed", meta);
              closeFuture.complete(null);
              return null;
            });
    return closeFuture;
  }

  @Override
  public PartitionStatus status() {
    return this.status.get();
  }

  @Override
  public PartitionMeta meta() {
    return this.meta;
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }

  class CursorSegment {
    private byte[] NOOP = new byte[1];
    private int slotSegment;
    private byte[] inflightCursors;
    private byte[] commitCursors;
    private AtomicLong commitVersion = new AtomicLong();
    private volatile long savedCommitVersion = 0;

    public CursorSegment(int slotSegment, byte[] cursors) {
      this.slotSegment = slotSegment;
      if (cursors == null || (cursors.length == 1 && NOOP.equals(cursors))) {
        // 避免没有的也初始化，因为要扫描肯定会读一次，可以加上轻量化的初始化，一个 'empty' array 既可以避免没有 timer 的 partition
        // 过多的占用内存空间。
        cursors = new byte[TIMER_WHEEL_SEGMENT * TIMER_CURSOR_SIZE];
        byte[] NOOP_BYTES = Cursor.NOOP.toBytes();
        for (int i = 0; i < TIMER_WHEEL_SEGMENT; i++) {
          System.arraycopy(NOOP_BYTES, 0, cursors, i * TIMER_CURSOR_SIZE, TIMER_CURSOR_SIZE);
        }
      }
      inflightCursors = new byte[cursors.length];
      System.arraycopy(cursors, 0, inflightCursors, 0, cursors.length);
      commitCursors = new byte[cursors.length];
      System.arraycopy(cursors, 0, commitCursors, 0, cursors.length);
    }

    public synchronized void putInflightCursor(int slot, byte[] cursor) {
      putCursor(slot, cursor, inflightCursors);
    }

    public synchronized byte[] getInflightCursor(int slot) {
      return getCursor(slot, inflightCursors);
    }

    public synchronized void putCommitCursor(int slot, byte[] cursor) {
      putCursor(slot, cursor, commitCursors);
      commitVersion.incrementAndGet();
    }

    public synchronized byte[] getCommitCursor(int slot) {
      return getCursor(slot, commitCursors);
    }

    public synchronized CompletableFuture<Void> trigger(long timestamp) {
      // trigger 和 构建串行，就无并发问题。
      long timestampSecs = timestamp / 1000;
      int slot = (int) (timestampSecs % TIMER_WINDOW);
      byte[] cursorBytes = getCommitCursor(slot);
      Cursor cursor = Cursor.get(cursorBytes);
      if (Cursor.NOOP.equals(cursor)) {
        return CompletableFuture.completedFuture(null);
      }
      putInflightCursor((int) (timestampSecs % TIMER_WINDOW), NOOP_CURSOR);
      putCommitCursor((int) (timestampSecs % TIMER_WINDOW), NOOP_CURSOR);
      append(
          BatchRecord.newBuilder()
              .setFlag(INVISIBLE_FLAG | TIMER_INDEX_TRIGGER)
              .addRecords(
                  Record.newBuilder()
                      .putHeaders(
                          TIMER_TRIGGER_TIMESTAMP_HEADER,
                          ByteString.copyFrom(Utils.getBytes(timestamp)))
                      .putHeaders(TIMER_TRIGGER_CURSOR_HEADER, ByteString.copyFrom(cursorBytes))
                      .build())
              .build());
      List<TimerIndex> timerIndexes = new LinkedList<>();
      return getTimerSlot0(cursor, timerIndexes)
          .thenAccept(
              nil -> {
                List<TimerIndex> matchedTimerIndexes = new LinkedList<>();
                for (TimerIndex timerIndex : timerIndexes) {
                  if (timerIndex.getTimestamp() > timestamp) {
                    timerIndexQueue.add(timerIndex);
                    continue;
                  }
                  matchedTimerIndexes.add(timerIndex);
                }
              })
          .thenAccept(nil -> trigger0(timerIndexes));
    }

    protected CompletableFuture<Void> getTimerSlot0(Cursor cursor, List<TimerIndex> timerIndexes) {
      CompletableFuture<List<BatchRecord>> indexesFuture =
          segments.get(cursor.getIndex()).getTimerSlot(cursor.getOffset());
      return indexesFuture.thenCompose(
          records -> {
            if (records == null || records.size() == 0) {
              return CompletableFuture.completedFuture(null);
            }
            for (BatchRecord record : records) {
              timerIndexes.addAll(
                  TimerIndex.getTimerIndexes(record.getRecords(0).getValue().toByteArray()));
            }
            Cursor prev = TimerUtils.getPreviousCursor(records.get(records.size() - 1));
            if (Cursor.NOOP.equals(prev)) {
              return CompletableFuture.completedFuture(null);
            }
            return getTimerSlot0(prev, timerIndexes);
          });
    }

    protected CompletableFuture<Void> trigger0(List<TimerIndex> timerIndexes) {
      // TODO: support record key
      BatchRecord.Builder builder = BatchRecord.newBuilder().setFlag(LINK_FLAG);
      for (TimerIndex timerIndex : timerIndexes) {
        ByteBuffer buf = ByteBuffer.allocate(4 + 8);
        buf.putInt(timerIndex.getIndex());
        buf.putLong(timerIndex.getOffset());
        builder.addRecords(Record.newBuilder().setValue(ByteString.copyFrom(buf.array())).build());
        log.info(
            "[TIMER_TRIGGER]tim={},topic={},partition={},index={},offset={}",
            timerIndex.getTimestamp(),
            meta.getTopic(),
            meta.getPartition(),
            timerIndex.getIndex(),
            timerIndex.getOffset());
      }
      return append(builder.build()).thenAccept(r -> {});
    }

    public synchronized void save() {
      long commitVersion = this.commitVersion.get();
      if (savedCommitVersion == commitVersion) {
        return;
      }
      append(
          BatchRecord.newBuilder()
              .setFlag(INVISIBLE_FLAG | META_FLAG)
              .addRecords(
                  Record.newBuilder()
                      .setKey(ByteString.copyFrom(getTimerSlotSegmentKey(slotSegment)))
                      .setValue(ByteString.copyFrom(commitCursors))
                      .build())
              .build());
      savedCommitVersion = commitVersion;
    }

    private void putCursor(int slot, byte[] cursor, byte[] cursors) {
      System.arraycopy(
          cursor, 0, cursors, slot % TIMER_WHEEL_SEGMENT * TIMER_CURSOR_SIZE, TIMER_CURSOR_SIZE);
    }

    private byte[] getCursor(int slot, byte[] cursors) {
      byte[] cursor = new byte[TIMER_CURSOR_SIZE];
      System.arraycopy(
          cursors, slot % TIMER_WHEEL_SEGMENT * TIMER_CURSOR_SIZE, cursor, 0, TIMER_CURSOR_SIZE);
      return cursor;
    }
  }
}
