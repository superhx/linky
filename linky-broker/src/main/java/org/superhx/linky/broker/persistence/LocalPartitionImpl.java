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

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class LocalPartitionImpl implements Partition {
  private static final int TIMER_WINDOW = (int) TimeUnit.DAYS.toSeconds(1);
  private static final int TIMER_WHEEL_SEGMENT = (int) TimeUnit.MINUTES.toSeconds(10);
  /** (timestamp, index, offset) */
  private static final int TIMER_INDEX_SIZE = 8 + 4 + 8;

  private static final String TIMER_SLOT_RECORD_HEADER = "TS";
  private static final String TIMER_PRE_CURSOR_HEADER = "TC";
  private static final ScheduledExecutorService schedule =
      Utils.newScheduledThreadPool(1, "TIMER_SAVE");

  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private PartitionMeta meta;
  private AtomicReference<PartitionStatus> status = new AtomicReference<>(PartitionStatus.NOOP);
  private List<Segment> segments;
  private volatile Segment lastSegment;
  private LocalSegmentManager localSegmentManager;
  private CompletableFuture<Segment> lastSegmentFuture;
  private Map<Segment, CompletableFuture<Segment>> nextSegmentFutures = new ConcurrentHashMap<>();
  private ReentrantLock appendLock = new ReentrantLock();
  private Queue<TimerIndex> timerIndexQueue = new LinkedBlockingQueue<>();

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    TimerIndex timerIndex = new TimerIndex().setTimestamp(batchRecord.getVisibleTimestamp());
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
//                  timerIndexQueue.offer(timerIndex);
                  ByteBuffer cursor = ByteBuffer.allocate(4 + 8);
                  cursor.putInt(appendResult.getIndex());
                  cursor.putLong(appendResult.getOffset());
                  commitedCursor = cursor.array();
                  return new AppendResult(cursor.array());
                default:
                  return new AppendResult(AppendStatus.FAIL);
              }
            });
  }

  protected void buildTimerIndex() throws Exception {

    Cursor expectedNextCursor = this.expectedNextCursor;
    Map<Integer, List<TimerIndex>> timestamp2Index = new HashMap<>();
    for (TimerIndex timerIndex = timerIndexQueue.poll();
        timerIndex != null;
        timerIndex = timerIndexQueue.poll()) {
      if (expectedNextCursor == null
          || timerIndex.getIndex() != expectedNextCursor.getIndex()
              && timerIndex.getOffset() != expectedNextCursor.getOffset()) {
        log.debug(
            "[TIMER_INDEX_TEMP_SKIP]timerIndex={},expectedNextCursor={}",
            timerIndex,
            expectedNextCursor);
      }

      // 不为timer的也要处理，用于推进 slo
      if (timerIndex.getTimestamp() == 0) {
        continue;
      }
      int slot = (int) (timerIndex.getTimestamp() % TIMER_WINDOW);
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
      byte[] indexesBytes = new byte[indexes.size() * TIMER_INDEX_SIZE];
      ByteBuffer indexesBuf = ByteBuffer.wrap(indexesBytes);
      for (TimerIndex timerIndex : indexes) {
        indexesBuf.putLong(timerIndex.getTimestamp());
        indexesBuf.putInt(timerIndex.getIndex());
        indexesBuf.putLong(timerIndex.getOffset());
      }
      BatchRecord.Builder indexesRecord = BatchRecord.newBuilder();
      indexesRecord
          .setFlag(Constants.TIMER_INDEX_FLAG)
          .addRecords(
              Record.newBuilder()
                  .putHeaders(
                      TIMER_SLOT_RECORD_HEADER,
                      BaseEncoding.base16().encode(ByteBuffer.allocate(4).putInt(slot).array()))
                  .putHeaders(
                      TIMER_PRE_CURSOR_HEADER,
                      BaseEncoding.base16().encode(getInflightTimerCursor(slot)))
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

  private long TIMER_CURSOR_SNAPSHOT_INTERVAL_MILLS = TimeUnit.SECONDS.toMillis(10);
  private volatile Cursor expectedNextCursor;
  private Map<Integer, byte[]> inflightCursors = new ConcurrentHashMap<>();
  private Map<Integer, byte[]> commitedCursors = new ConcurrentHashMap<>();
  private byte[] commitedCursor;
  private long lastTimerCursorSnapshotTimestamp = System.currentTimeMillis();

  private byte[] getInflightTimerCursor(int slot) {
    return null;
  }

  private void putInflightTimerCursor(int slot, byte[] cursor) {}

  private void putCommitTimerCursor(int slot, byte[] cursor) {}

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

      lastSegmentFuture = new CompletableFuture();
      localSegmentManager
          .nextSegment(
              meta.getTopicId(),
              meta.getPartition(),
              segments.isEmpty() ? Segment.NO_INDEX : segments.get(segments.size() - 1).getIndex())
          .thenAccept(
              s -> {
                lastSegmentFuture.complete(s);
                this.lastSegment = s;
                segments.add(this.lastSegment);
              });
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
                segments.add(s);
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
    Segment segment = null;
    for (Segment seg : segments) {
      if (seg.getIndex() == segmentIndex) {
        if (seg.isSealed() && seg.getEndOffset() <= offset) {
          segmentIndex++;
          offset = 0;
          continue;
        }
        segment = seg;
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
    return recordFuture.thenApply(
        r -> {
          if (r == null) {
            getResult.setStatus(GetStatus.NO_NEW_MSG);
            getResult.setNextCursor(cursor);
          } else {
            getResult.setBatchRecord(r);
            nextCursor.putLong(finalOffset + r.getRecordsCount());
            getResult.setNextCursor(nextCursor.array());
          }
          return getResult;
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
      for (int i = segments.size() - 1; i >= 0; i--) {
        Segment segment = segments.get(i);
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
    if (!status.compareAndSet(PartitionStatus.NOOP, PartitionStatus.OPENING)) {
      log.warn("cannot open {} status partition {}", status.get(), meta);
      return CompletableFuture.completedFuture(this.status.get());
    }

    log.info("partition {} opening...", meta);
    CompletableFuture<PartitionStatus> openFuture = new CompletableFuture<>();
    localSegmentManager
        .getSegments(meta.getTopicId(), meta.getPartition())
        .thenCompose(
            s -> {
              segments = new CopyOnWriteArrayList<>(s);
              Collections.sort(segments, Comparator.comparingInt(Segment::getIndex));
              if (!segments.isEmpty()) {
                return segments.get(segments.size() - 1).seal();
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenAccept(
            n -> {
              this.status.set(PartitionStatus.OPEN);
              log.info("partition {} opened", meta);
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
}
