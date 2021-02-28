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
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.Record;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Constants.*;
import static org.superhx.linky.broker.persistence.Partition.AppendContext.TIMERSTAMP_CTX_KEY;
import static org.superhx.linky.broker.persistence.Partition.AppendContext.TIMER_INDEX_CTX_KEY;

class Timer implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(Timer.class);

  private Partition partition;
  private Queue<TimerIndex> timerIndexQueue = new LinkedBlockingQueue<>();
  private Map<Integer, TimerCursorSegment> cursorSegments = new ConcurrentHashMap<>();
  private volatile boolean timerEnable = false;
  private volatile long timestampBarrier = -1L;
  private volatile Cursor timestampBarrierCursor = null;
  private volatile long safeTriggerTimestamp = -1L;

  // LOAD & SAVE
  private volatile long timerNextTimestamp = -1L;

  private volatile Cursor nextTimerIndexBuildCursor;
  // LOAD & SAVE
  private volatile Cursor timerIndexBuildLSO;

  private volatile Cursor timerCommitLSO;

  private volatile boolean catching;

  private BlockingQueue<BatchRecord> waitingRetryTimerIndexes = new LinkedBlockingQueue<>();

  private AtomicReference<Status> status = new AtomicReference<>(Status.RECOVER);

  private BlockingQueue<TimerCommit> timerCommitQueue = new LinkedBlockingQueue<>();

  enum Status {
    RECOVER,
    START
  }

  public Timer(Partition partition) {
    this.partition = partition;
    partition.registerAppendHook(getAppendHook());
  }

  private Partition.AppendHook getAppendHook() {
    return new Partition.AppendHook() {
      @Override
      public void before(Partition.AppendContext ctx, BatchRecord.Builder batchRecord) {
        TimerIndex timerIndex;
        long timestampBarrier = Timer.this.timestampBarrier;
        long visibleTimestamp =
            (batchRecord.getVisibleTimestamp() - TIMESTAMP_BARRIER_SAFE_WINDOW) / 1000 * 1000;
        if (visibleTimestamp <= 0) {
          timerIndex = TimerUtils.getTimerIndex(batchRecord);
        } else if (visibleTimestamp <= Math.max(timestampBarrier, System.currentTimeMillis())) {
          timerIndex = TimerUtils.getTimerIndex(batchRecord);
        } else {
          batchRecord.setFlag(batchRecord.getFlag() | INVISIBLE_FLAG | TIMER_FLAG);
          timerIndex = TimerUtils.getTimerIndex(batchRecord);
          ctx.putContext(TIMERSTAMP_CTX_KEY, batchRecord.getVisibleTimestamp());
          if (!timerEnable) {
            try {
              enableTimer();
            } catch (Exception e) {
              throw new LinkyIOException(e);
            }
          }
        }
        ctx.putContext(TIMER_INDEX_CTX_KEY, timerIndex);
      }

      @Override
      public void after(Partition.AppendContext ctx) {
        TimerIndex timerIndex = ctx.getContext(TIMER_INDEX_CTX_KEY);
        Cursor cursor = ctx.getCursor();
        timerIndex.setCursor(cursor.getIndex(), cursor.getOffset());
        timerIndex.setNext(ctx.getNextCursor());
        Long timestamp = ctx.getContext(TIMERSTAMP_CTX_KEY);
        if (timestamp != null) {
          timerIndex.setIndexes(
              TimerUtils.getTimerIndexBytes(
                  ctx.getContext(TIMERSTAMP_CTX_KEY), cursor.getIndex(), cursor.getOffset()));
        }
        asyncBuildTimerIndex(timerIndex);
      }
    };
  }

  public void init() {
    byte[] lsoBytes = partition.getMeta(TIMER_LSO_KEY);
    if (lsoBytes == null) {
      timerEnable = false;
      status.set(Status.START);
      return;
    }
    log.info("[TIMER_INIT]{},timerOn", partition.name());
    timerIndexBuildLSO = Cursor.get(lsoBytes);
    timerCommitLSO = Cursor.get(lsoBytes);
    nextTimerIndexBuildCursor = timerIndexBuildLSO;
    timestampBarrierCursor = timerIndexBuildLSO;
    byte[] timerNextTimestampBytes = partition.getMeta(TIMER_NEXT_TIMESTAMP_KEY);
    timerNextTimestamp = Utils.getLong(timerNextTimestampBytes);
    timestampBarrier = timerNextTimestamp;
    safeTriggerTimestamp = timerNextTimestamp;
    status.set(Status.RECOVER);
    log.info(
        "[TIMER_META_LOAD]{},timerLSO={},timerNextTimestamp={}",
        partition.name(),
        timerIndexBuildLSO,
        timerNextTimestamp);
    new Recover(partition.getNextCursor()).run();
    timerEnable = true;
  }

  public void shutdown() {
    flushMeta();
    log.info(
        "[TIMER_META_SAVE]{},timerIndexBuildLso={},timerNextTimestamp={}",
        partition.name(),
        timerIndexBuildLSO,
        timerNextTimestamp);
  }

  private CompletableFuture<Void> flushMeta() {
    return CompletableFuture.allOf(
            cursorSegments.values().stream()
                .map(s -> s.flushMeta())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .thenCompose(
            nil ->
                partition.setMeta(
                    Constants.TIMER_LSO_KEY,
                    getTimerLSO().toBytes(),
                    Constants.TIMER_NEXT_TIMESTAMP_KEY,
                    Utils.getBytes(timerNextTimestamp)));
  }

  private Cursor getTimerLSO() {
    return timerIndexBuildLSO.compareTo(timerCommitLSO) < 0 ? timerIndexBuildLSO : timerCommitLSO;
  }

  // TODO: async enable
  private synchronized void enableTimer() throws Exception {
    if (timerEnable) {
      return;
    }
    timerEnable = true;
    log.info("[ENABLE_TIMER]{}", partition.name());
    timerIndexBuildLSO = partition.getNextCursor();
    timerCommitLSO = timerIndexBuildLSO;
    nextTimerIndexBuildCursor = timerIndexBuildLSO;
    timestampBarrierCursor = timerIndexBuildLSO;

    timerNextTimestamp = System.currentTimeMillis() / 1000 * 1000;
    timestampBarrier = timerNextTimestamp;
    safeTriggerTimestamp = timerNextTimestamp;

    partition
        .setMeta(
            Constants.TIMER_LSO_KEY,
            timerIndexBuildLSO.toBytes(),
            Constants.TIMER_NEXT_TIMESTAMP_KEY,
            Utils.getBytes(timerNextTimestamp))
        .get();
  }

  private ReentrantLock buildIndexLock = new ReentrantLock();
  private ExecutorService executorService = Utils.newFixedThreadPool(1, "TimerCatchup");
  private volatile Cursor expectedCatchupCursor;

  public void asyncBuildTimerIndex(TimerIndex timerIndex) {
    expectedCatchupCursor = timerIndex.getCursor();
    if (!timerEnable) {
      return;
    }
    if (status.get() != Status.START) {
      return;
    }
    buildIndexLock.lock();
    try {
      if (catching) {
        if (log.isDebugEnabled()) {
          log.debug("[ASYNC_BUILD_INDEX]{},SKIP,IS_CATCHING", partition.name());
        }
        return;
      }
      if (nextTimerIndexBuildCursor.compareTo(timerIndex.getCursor()) < 0) {
        catchUpBuildTimerIndex(timerIndex.getCursor());
      }
      timerIndexQueue.add(timerIndex);

      Cursor next = timerIndex.getNext();
      nextTimerIndexBuildCursor.setIndex(next.getIndex());
      nextTimerIndexBuildCursor.setOffset(next.getOffset());
    } finally {
      buildIndexLock.unlock();
    }
  }

  private volatile CompletableFuture<Void> lastRunningCF;

  public void process() {
    if (!timerEnable) {
      return;
    }
    if (lastRunningCF != null) {
      return;
    }
    lastRunningCF =
        buildTimerIndex()
            .thenCompose(
                nil -> {
                  if (waitingRetryTimerIndexes.size() == 0) {
                    return prepare();
                  } else {
                    return CompletableFuture.completedFuture(null);
                  }
                });
    lastRunningCF.handle(
        (nil, t) -> {
          if (t != null) {
            log.warn("[TIMER_PROCESS_FAIL]", t);
          }
          lastRunningCF = null;
          return null;
        });
  }

  private byte[] getTimerCursor(int slot) {
    TimerCursorSegment timerCursorSegment = getTimerCursorSegment(slot);
    byte[] cursor = timerCursorSegment.getCursor(slot);
    return cursor;
  }

  private void putTimerCursor(int slot, byte[] cursor) {
    int slotSegment = slot / TIMER_WHEEL_SEGMENT;
    TimerCursorSegment timerCursorSegment = cursorSegments.get(slotSegment);
    timerCursorSegment.putCursor(slot, cursor);
  }

  private TimerCursorSegment getTimerCursorSegment(int slot) {
    int slotSegment = slot / TIMER_WHEEL_SEGMENT;
    TimerCursorSegment timerCursorSegment = cursorSegments.get(slotSegment);
    try {
      if (timerCursorSegment == null) {
        timerCursorSegment =
            new TimerCursorSegment(
                slotSegment, partition.getMeta(TimerUtils.getTimerSlotSegmentKey(slotSegment)));
        cursorSegments.put(slotSegment, timerCursorSegment);
      }
      return timerCursorSegment;
    } catch (Exception e) {
      e.printStackTrace();
      throw new LinkyIOException(e);
    }
  }

  protected CompletableFuture<Void> buildTimerIndex() {
    if (waitingRetryTimerIndexes.size() != 0) {
      List<BatchRecord> records = new ArrayList<>(waitingRetryTimerIndexes.size());
      waitingRetryTimerIndexes.drainTo(records);
      AppendPipeline pipeline = partition.appendPipeline();
      return CompletableFuture.allOf(
          records.stream()
              .map(r -> appendTimerIndex(pipeline, TimerUtils.getSlot(r), r))
              .collect(Collectors.toList())
              .toArray(new CompletableFuture[0]));
    }

    Map<Integer, List<TimerIndex>> timestamp2Index = new HashMap<>();
    Cursor maxCursor = Cursor.NOOP;
    for (TimerIndex timerIndex = timerIndexQueue.poll();
        timerIndex != null;
        timerIndex = timerIndexQueue.poll()) {
      if (maxCursor.compareTo(timerIndex.getNext()) < 0) {
        maxCursor = timerIndex.getNext();
      }
      if (!timerIndex.isTimer()) {
        continue;
      }
      int slot = timerIndex.getSlot();
      List<TimerIndex> indexes = timestamp2Index.get(slot);
      if (indexes == null) {
        indexes = new LinkedList<>();
        timestamp2Index.put(slot, indexes);
      }
      indexes.add(timerIndex);
    }

    Cursor timerIndexBuildLSO = this.timerIndexBuildLSO;
    if (maxCursor.compareTo(timerIndexBuildLSO) > 0) {
      timerIndexBuildLSO = maxCursor;
    }
    Cursor barrier = timerIndexBuildLSO;
    return buildTimerIndex0(timestamp2Index)
        .thenAccept(
            nil -> {
              updateTimestampBarrier(barrier);
              this.timerIndexBuildLSO = barrier;
            });
  }

  private static final long MAX_SAFE_TIMESTAMP_DIFF = TimeUnit.SECONDS.toMillis(30);

  private void updateTimestampBarrier(Cursor maxTimerIndexBuildCursor) {
    if (System.currentTimeMillis() - safeTriggerTimestamp > MAX_SAFE_TIMESTAMP_DIFF) {
      log.warn(
          "[SAFE_TIMESTAMP_DIFF]safe={},diff={}",
          safeTriggerTimestamp,
          System.currentTimeMillis() - safeTriggerTimestamp);
    }
    if (maxTimerIndexBuildCursor.compareTo(timestampBarrierCursor) < 0) {
      return;
    }
    safeTriggerTimestamp = timestampBarrier;
    long newTimestampBarrier =
        (System.currentTimeMillis() + TIMESTAMP_BARRIER_SAFE_WINDOW) / 1000 * 1000;
    if (newTimestampBarrier <= timestampBarrier) {
      return;
    }
    timestampBarrier = newTimestampBarrier;
    // TODO: inflight 如果回退了，可以修正 timestampBarrierCursor
    timestampBarrierCursor = partition.getNextCursor();
  }

  private CompletableFuture<Void> buildTimerIndex0(Map<Integer, List<TimerIndex>> timestamp2Index) {
    if (timestamp2Index.size() == 0) {
      return CompletableFuture.completedFuture(null);
    }

    List<CompletableFuture<Void>> futures = new LinkedList<>();
    AppendPipeline pipeline = partition.appendPipeline();
    for (Map.Entry<Integer, List<TimerIndex>> entry : timestamp2Index.entrySet()) {
      int slot = entry.getKey();
      List<TimerIndex> indexes = entry.getValue();
      byte[] indexesBytes = TimerUtils.toTimerIndexBytes(indexes);
      if (indexesBytes.length == 0) {
        continue;
      }
      BatchRecord indexesRecord =
          BatchRecord.newBuilder()
              .setFlag(INVISIBLE_FLAG | TIMER_FLAG)
              .addRecords(
                  Record.newBuilder()
                      .putHeaders(TIMER_TYPE_HEADER, ByteString.copyFrom(TIMER_INDEX_TYPE))
                      .putHeaders(
                          TIMER_SLOT_RECORD_HEADER, ByteString.copyFrom(Utils.getBytes(slot)))
                      .putHeaders(
                          TIMER_PRE_CURSOR_HEADER, ByteString.copyFrom(getTimerCursor(slot)))
                      .setValue(ByteString.copyFrom(indexesBytes))
                      .build())
              .build();

      futures.add(appendTimerIndex(pipeline, slot, indexesRecord));
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  private CompletableFuture<Void> appendTimerIndex(
      AppendPipeline pipeline, int slot, BatchRecord batchRecord) {
    ByteBuffer cursor = ByteBuffer.allocate(4 + 8);
    Segment.AppendContext context =
        new Segment.AppendContext()
            .setHook(
                new Segment.AppendHook() {
                  @Override
                  public void before(Segment.AppendContext context, BatchRecord record) {
                    cursor.putInt(context.getIndex());
                    cursor.putLong(context.getOffset());
                    putTimerCursor(slot, cursor.array());
                  }
                });
    return pipeline
        .append(context, batchRecord)
        .thenAccept(
            appendResult -> {
              switch (appendResult.getStatus()) {
                case SUCCESS:
                  putTimerCursor(slot, cursor.array());
                  break;
                default:
                  throw new LinkyIOException(
                      String.format(
                          "[TIMER_INDEX_APPEND_ERROR]{},{}",
                          partition.name(),
                          appendResult.getStatus()));
              }
            })
        .exceptionally(
            t -> {
              log.warn("[TIMER_INDEX_BUILD_FAIL]{}", partition.name(), t);
              waitingRetryTimerIndexes.add(batchRecord);
              return null;
            });
  }

  private void catchUpBuildTimerIndex(Cursor toCursor) {
    if (catching) {
      return;
    }
    log.info(
        "[CATCHUP_BUILD_TIMER_INDEX]{},from={},to={}",
        partition.name(),
        nextTimerIndexBuildCursor,
        toCursor);
    catching = true;
    executorService.submit(
        () -> {
          CompletableFuture<Void> cf = catchupBuildTimerIndex0(nextTimerIndexBuildCursor, toCursor);
          cf.thenAccept(nil -> catching = false)
              .exceptionally(
                  t -> {
                    log.error("[CATCHUP_BUILD_TIMER_INDEX_FAIL]{}", partition.name(), t);
                    catching = false;
                    return null;
                  });
        });
  }

  private CompletableFuture<Void> catchupBuildTimerIndex0(Cursor cursor, Cursor toCursor) {
    if (cursor.compareTo(toCursor) > 0) {
      return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Cursor> nextCursor =
        partition
            .get(cursor.toBytes(), false, false)
            .thenApply(
                getResult -> {
                  if (getResult.getStatus() == Partition.GetStatus.NO_NEW_MSG) {
                    throw new LinkyIOException(
                        String.format(
                            "[CATCHUP_UNEXPECTED]%s,NO_NEW_MSG,%s", partition.name(), cursor));
                  }
                  timerIndexQueue.add(TimerUtils.getTimerIndex(getResult.getBatchRecord()));
                  Cursor next = Cursor.get(getResult.getNextCursor());
                  nextTimerIndexBuildCursor.setIndex(next.getIndex());
                  nextTimerIndexBuildCursor.setOffset(next.getOffset());
                  return Cursor.get(getResult.getNextCursor());
                });
    return nextCursor.thenAccept(c -> catchupBuildTimerIndex0(c, toCursor));
  }

  private CompletableFuture<Void> prepare() {
    if (timerCommitQueue.size() == 0) {
      timerCommitLSO = partition.getNextCursor();
    }
    long safeTriggerTimestamp = this.safeTriggerTimestamp;
    long toTimestamp = Math.min(safeTriggerTimestamp, System.currentTimeMillis() / 1000 * 1000);
    if (toTimestamp - timerNextTimestamp < 1000L) {
      return CompletableFuture.completedFuture(null);
    }
    AppendPipeline pipeline = null;
    long timestamp = timerNextTimestamp;
    for (; timestamp <= toTimestamp; timestamp += 1000) {
      long timestampSecs = timestamp / 1000;
      int slot = (int) (timestampSecs % TIMER_WINDOW);
      byte[] cursorBytes = getTimerCursor(slot);
      Cursor cursor = Cursor.get(cursorBytes);
      if (Cursor.NOOP.equals(cursor)) {
        continue;
      }
      if (pipeline == null) {
        pipeline = partition.appendPipeline();
      }
      try {
        Partition.AppendResult appendResult =
            pipeline
                .append(
                    BatchRecord.newBuilder()
                        .setFlag(INVISIBLE_FLAG | TIMER_FLAG)
                        .addRecords(
                            Record.newBuilder()
                                .putHeaders(
                                    TIMER_TYPE_HEADER, ByteString.copyFrom(TIMER_PREPARE_TYPE))
                                .putHeaders(
                                    TIMER_TIMESTAMP_HEADER,
                                    ByteString.copyFrom(Utils.getBytes(timestamp)))
                                .putHeaders(
                                    TIMER_PREPARE_CURSOR_HEADER, ByteString.copyFrom(cursorBytes))
                                .build())
                        .build())
                .get();
        if (log.isDebugEnabled()) {
          log.debug(
              "[TIMER_PREPARE]tim={},cursor={},status={}",
              timestamp,
              cursor,
              appendResult.getStatus());
        }
        if (appendResult.getStatus() != Partition.AppendStatus.SUCCESS) {
          log.warn(
              "[TIMER_PREPARE_FAIL]tim={},cursor={},status={}",
              timestamp,
              cursor,
              appendResult.getStatus());
          break;
        }
        timerCommitQueue.offer(new TimerCommit(timestamp, cursor));
        putTimerCursor(slot, NOOP_CURSOR);
      } catch (Exception e) {
        log.warn("[TIMER_PREPARE_FAIL]tim={},cursor={}", timestamp, cursor, e);
        break;
      }
    }
    timerNextTimestamp = timestamp;

    List<TimerCommit> timerCommits = new LinkedList<>();
    timerCommitQueue.drainTo(timerCommits);
    for (TimerCommit tc : timerCommits) {
      if (tc.isDone()) {
        continue;
      }
      tc.run();
      timerCommitQueue.offer(tc);
    }

    return CompletableFuture.completedFuture(null);
  }

  class TimerCursorSegment {
    private int slotSegment;
    private byte[] cursors;
    private AtomicLong commitVersion = new AtomicLong();
    private volatile long savedCommitVersion = 0;
    private volatile boolean saving = false;

    public TimerCursorSegment(int slotSegment, byte[] cursors) {
      this.slotSegment = slotSegment;
      if (cursors == null || (cursors.length == 1 && NOOP_CURSOR_SEGMENT.equals(cursors))) {
        cursors = NOOP_CURSOR_SEGMENT;
      }
      this.cursors = new byte[cursors.length];
      System.arraycopy(cursors, 0, this.cursors, 0, cursors.length);
    }

    public synchronized void putCursor(int slot, byte[] cursor) {
      if (Arrays.equals(NOOP_CURSOR_SEGMENT, cursors)) {
        cursors = new byte[TIMER_WHEEL_SEGMENT * TIMER_CURSOR_SIZE];
        for (int i = 0; i < TIMER_WHEEL_SEGMENT; i++) {
          System.arraycopy(
              Constants.NOOP_CURSOR, 0, cursors, i * TIMER_CURSOR_SIZE, TIMER_CURSOR_SIZE);
        }
      }
      putCursor(slot, cursor, cursors);
    }

    public synchronized byte[] getCursor(int slot) {
      if (Arrays.equals(NOOP_CURSOR_SEGMENT, cursors)) {
        return NOOP_CURSOR;
      }
      return getCursor(slot, cursors);
    }

    public synchronized CompletableFuture<Void> flushMeta() {
      if (saving) {
        CompletableFuture future = new CompletableFuture();
        future.completeExceptionally(
            new LinkyIOException(
                String.format("[TIMER_CURSOR_SEGMENT_SAVING]slot=%s", slotSegment)));
        return future;
      }
      long commitVersion = this.commitVersion.get();
      if (savedCommitVersion == commitVersion) {
        return CompletableFuture.completedFuture(null);
      }
      saving = true;
      CompletableFuture<Void> future =
          partition
              .append(
                  BatchRecord.newBuilder()
                      .setFlag(INVISIBLE_FLAG | META_FLAG)
                      .addRecords(
                          Record.newBuilder()
                              .setKey(
                                  ByteString.copyFrom(
                                      TimerUtils.getTimerSlotSegmentKey(slotSegment)))
                              .setValue(ByteString.copyFrom(cursors))
                              .build())
                      .build())
              .thenAccept(
                  rst -> {
                    if (rst.getStatus() != Partition.AppendStatus.SUCCESS) {
                      throw new LinkyIOException(
                          String.format("[TIMER_LINK_APPEND_FAIL]%s", rst.getStatus()));
                    }
                    savedCommitVersion = commitVersion;
                  });
      future.handle(
          (nil, t) -> {
            saving = false;
            return null;
          });
      return future;
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

  class TimerCommit implements Runnable {
    private long timestamp;
    private Cursor cursor;
    private volatile CompletableFuture<List<TimerIndex>> timerIndexesCF;
    private volatile CompletableFuture<Void> commitCF;
    private volatile boolean done;
    private volatile boolean linked;

    public TimerCommit(long timestamp, Cursor cursor) {
      this.timestamp = timestamp;
      this.cursor = cursor;
    }

    public boolean isDone() {
      return done;
    }

    public void setLinked() {
      linked = true;
    }

    @Override
    public synchronized void run() {
      if (done) {
        return;
      }
      if (commitCF != null) {
        return;
      }
      // TODO: async, get timer slot 如果是本地的话就是同步的，因此从语义来说，所有返回 completeFuture 的接口应该内部用线程做一个假异步
      commitCF =
          getTimerSlot()
              .thenCompose(timerIndexes -> commit(timerIndexes))
              .thenAccept(nil -> done = true)
              .exceptionally(
                  t -> {
                    log.warn("[TIMER_COMMIT_FAIL]tim={},cursor={}", timestamp, cursor, t);
                    commitCF = null;
                    return null;
                  });
    }

    private synchronized CompletableFuture<List<TimerIndex>> getTimerSlot() {
      if (timerIndexesCF == null) {
        timerIndexesCF = partition.getTimerSlot(cursor);
      }
      timerIndexesCF.exceptionally(
          t -> {
            t.printStackTrace();
            timerIndexesCF = null;
            return null;
          });
      return timerIndexesCF;
    }

    private synchronized CompletableFuture<Void> commit(List<TimerIndex> timerIndexes) {
      List<TimerIndex> matchedTimerIndexes = new LinkedList<>();
      List<TimerIndex> unmatchedTimerIndexes = new LinkedList<>();

      for (TimerIndex timerIndex : timerIndexes) {
        timerIndex.split(timestamp, matchedTimerIndexes, unmatchedTimerIndexes);
      }
      // TODO: support record key
      AppendPipeline pipeline = partition.appendPipeline();
      BatchRecord.Builder link = BatchRecord.newBuilder().setFlag(LINK_FLAG | TIMER_FLAG);
      for (TimerIndex timerIndex : matchedTimerIndexes) {
        byte[] timerIndexBytes = timerIndex.getTimerIndexBytes();
        ByteBuffer buf = ByteBuffer.wrap(timerIndexBytes);
        for (int i = 0; i < timerIndexBytes.length / TIMER_INDEX_SIZE; i++) {
          log.info(
              "[TIMER_LINK]partition={},tim={},index={},offset={}",
              partition.name(),
              buf.getLong(),
              buf.getInt(),
              buf.getLong());
          link.addRecords(
              Record.newBuilder()
                  .putHeaders(
                      TIMER_TIMESTAMP_HEADER, ByteString.copyFrom(Utils.getBytes(timestamp)))
                  .setValue(
                      ByteString.copyFrom(
                          timerIndexBytes, i * TIMER_INDEX_SIZE + 8, TIMER_LINK_SIZE))
                  .build());
        }
      }

      // 分开是便于回收，timer index 调度完后空间就可以被回收了。
      BatchRecord.Builder commit =
          BatchRecord.newBuilder().setFlag(INVISIBLE_FLAG | TIMER_FLAG | META_FLAG);
      commit.addRecords(
          Record.newBuilder()
              .putHeaders(TIMER_TYPE_HEADER, ByteString.copyFrom(TIMER_COMMIT_TYPE))
              .putHeaders(TIMER_TIMESTAMP_HEADER, ByteString.copyFrom(Utils.getBytes(timestamp)))
              .setValue(ByteString.copyFrom(TimerUtils.toTimerIndexBytes(unmatchedTimerIndexes)))
              .build());

      List<CompletableFuture<Void>> futures = new LinkedList<>();
      if (!linked) {
        futures.add(
            pipeline
                .append(link.build())
                .thenAccept(
                    rst -> {
                      if (rst.getStatus() != Partition.AppendStatus.SUCCESS) {
                        throw new LinkyIOException(
                            String.format("[TIMER_LINK_APPEND_FAIL]%s", rst.getStatus()));
                      }
                    }));
      }
      futures.add(
          pipeline
              .append(commit.build())
              .thenAccept(
                  rst -> {
                    if (rst.getStatus() != Partition.AppendStatus.SUCCESS) {
                      throw new LinkyIOException(
                          String.format("[TIMER_COMMIT_APPEND_FAIL]%s", rst.getStatus()));
                    }
                    if (log.isDebugEnabled()) {
                      log.debug("[TIMER_COMMIT]{},{}", partition.name(), timestamp);
                    }
                  }));

      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
  }

  class Recover {
    private Cursor endCursor;
    private Map<Integer, Set<ByteBuffer>> waitingBuild = new HashMap<>();
    private Map<Long, TimerCommit> waitingCommit = new HashMap<>();
    private long maxCommitedTimestamp = -1L;

    public Recover(Cursor endCursor) {
      this.endCursor = endCursor;
    }

    public void run() {
      log.info("[TIMER_RECOVER_START]{}", partition.name());
      partition.get(
          timerIndexBuildLSO.toBytes(),
          endCursor.toBytes(),
          new StreamObserver<BatchRecord>() {
            @Override
            public void onNext(BatchRecord batchRecord) {
              Recover.this.onNext(batchRecord);
            }

            @Override
            public void onError(Throwable throwable) {
              log.error("TODO: Recover fail retry", throwable);
            }

            @Override
            public void onCompleted() {
              Recover.this.onComplete();
            }
          });
    }

    private void onNext(BatchRecord batchRecord) {
      if (!Flag.isTimer(batchRecord.getFlag())) {
        return;
      }
      // TIMER MSG
      if (batchRecord.getVisibleTimestamp() > 0) {
        addWaitingBuild(batchRecord);
        return;
      }
      // TIMER INDEX MSG
      if (TimerUtils.isTimerIndex(batchRecord)) {
        delWaitingBuild(batchRecord);
        return;
      }

      // TIMER PREPARE MSG
      if (TimerUtils.isTimerPrepare(batchRecord)) {
        addTimerCommit(batchRecord);
        return;
      }

      // TIMER LINK MSG
      if (TimerUtils.isTimerLink(batchRecord)) {
        setTimerLink(batchRecord);
        return;
      }

      // TIMER COMMIT MSG
      if (TimerUtils.isTimerCommit(batchRecord)) {
        delTimerCommit(batchRecord);
        return;
      }
    }

    private void onComplete() {
      Map<Integer, List<TimerIndex>> timerIndexes = new HashMap<>();
      for (Map.Entry<Integer, Set<ByteBuffer>> entry : waitingBuild.entrySet()) {
        int slot = entry.getKey();
        Set<ByteBuffer> indexes = entry.getValue();
        if (indexes.size() == 0) {
          continue;
        }
        int size = indexes.stream().mapToInt(b -> b.array().length).sum();
        ByteBuffer indexesBytes = ByteBuffer.allocate(size);
        indexes.forEach(b -> indexesBytes.put(b));
        TimerIndex timerIndex = new TimerIndex();
        timerIndex.setSlot(slot);
        timerIndex.setIndexes(indexesBytes.array());
        timerIndexes.put(slot, Arrays.asList(timerIndex));
      }
      buildTimerIndex0(timerIndexes)
          .thenAccept(
              nil -> {
                timerIndexBuildLSO = endCursor;
                nextTimerIndexBuildCursor = endCursor;
                status.set(Status.START);
                log.info("[TIMER_RECOVER_END]{}", partition.name());
              });
      timerNextTimestamp = Math.max(timerNextTimestamp, maxCommitedTimestamp);
      waitingCommit.values().forEach(tc -> timerCommitQueue.offer(tc));
    }

    private void addWaitingBuild(BatchRecord batchRecord) {
      int slot = TimerUtils.slot(batchRecord.getVisibleTimestamp());
      byte[] indexBytes =
          TimerUtils.getTimerIndexBytes(
              batchRecord.getVisibleTimestamp(),
              batchRecord.getIndex(),
              batchRecord.getFirstOffset());
      Set<ByteBuffer> indexes = waitingBuild.get(slot);
      if (indexes == null) {
        indexes = new HashSet<>();
        waitingBuild.put(slot, indexes);
      }
      indexes.add(ByteBuffer.wrap(indexBytes));
    }

    private void delWaitingBuild(BatchRecord batchRecord) {
      ByteString slotBytes =
          batchRecord.getRecords(0).getHeadersOrDefault(TIMER_SLOT_RECORD_HEADER, null);
      int slot = Utils.getInt(slotBytes.toByteArray());
      Set<ByteBuffer> indexes = waitingBuild.get(slot);
      if (indexes == null) {
        return;
      }
      byte[] indexesBytes = batchRecord.getRecords(0).getValue().toByteArray();
      for (int i = 0; i < indexesBytes.length / TIMER_INDEX_SIZE; i++) {
        byte[] indexBytes = new byte[TIMER_INDEX_SIZE];
        System.arraycopy(indexesBytes, i * TIMER_INDEX_SIZE, indexBytes, 0, TIMER_INDEX_SIZE);
        indexes.remove(ByteBuffer.wrap(indexBytes));
        if (log.isDebugEnabled()) {
          log.debug("[RECOVER_INDEX_BUILD_SKIP]{}", indexBytes);
        }
      }
    }

    private void addTimerCommit(BatchRecord batchRecord) {
      if (log.isDebugEnabled()) {
        log.debug("[RECOVER_SCAN_PREPARE]{}", batchRecord);
      }
      Record record = batchRecord.getRecords(0);
      long timestamp =
          Utils.getLong(record.getHeadersOrDefault(TIMER_TIMESTAMP_HEADER, null).toByteArray());
      maxCommitedTimestamp = Math.max(timestamp, maxCommitedTimestamp);
      byte[] timerCursor =
          record.getHeadersOrDefault(TIMER_PREPARE_CURSOR_HEADER, null).toByteArray();
      waitingCommit.put(timestamp, new TimerCommit(timestamp, Cursor.get(timerCursor)));
    }

    private void setTimerLink(BatchRecord batchRecord) {
      if (log.isDebugEnabled()) {
        log.debug("[RECOVER_SCAN_LINK]{}", batchRecord);
      }
      Record record = batchRecord.getRecords(0);
      long timestamp =
          Utils.getLong(record.getHeadersOrDefault(TIMER_TIMESTAMP_HEADER, null).toByteArray());
      TimerCommit timerCommit = waitingCommit.get(timestamp);
      if (timerCommit == null) {
        return;
      }
      timerCommit.setLinked();
    }

    private void delTimerCommit(BatchRecord batchRecord) {
      if (log.isDebugEnabled()) {
        log.debug("[RECOVER_SCAN_COMMIT]{}", batchRecord);
      }
      Record record = batchRecord.getRecords(0);
      long timestamp =
          Utils.getLong(record.getHeadersOrDefault(TIMER_TIMESTAMP_HEADER, null).toByteArray());
      waitingCommit.remove(timestamp);
      int slot = TimerUtils.slot(timestamp);
      Set<ByteBuffer> indexes = waitingBuild.get(slot);
      if (indexes == null) {
        indexes = new HashSet<>();
        waitingBuild.put(slot, indexes);
      }
      byte[] indexesBytes = batchRecord.getRecords(0).getValue().toByteArray();
      for (int i = 0; i < indexesBytes.length / TIMER_INDEX_SIZE; i++) {
        byte[] indexBytes = new byte[TIMER_INDEX_SIZE];
        System.arraycopy(indexesBytes, i * TIMER_INDEX_SIZE, indexBytes, 0, TIMER_INDEX_SIZE);
        indexes.add(ByteBuffer.wrap(indexBytes));
      }
    }
  }
}
