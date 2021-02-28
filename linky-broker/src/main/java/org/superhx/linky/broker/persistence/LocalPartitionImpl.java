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
import io.grpc.stub.StreamObserver;
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

import static org.superhx.linky.broker.persistence.Constants.INVISIBLE_FLAG;
import static org.superhx.linky.broker.persistence.Constants.META_FLAG;

public class LocalPartitionImpl implements Partition {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private static final ScheduledExecutorService TIMER_SCHEDULER =
      Utils.newScheduledThreadPool(1, "TimerScheduler");
  public final String partitionName;
  private PartitionMeta meta;
  private AtomicReference<PartitionStatus> status = new AtomicReference<>(PartitionStatus.NOOP);
  private NavigableMap<Integer, Segment> segments;
  private volatile Segment lastSegment;
  private LocalSegmentManager localSegmentManager;
  private CompletableFuture<Segment> lastSegmentFuture;
  private Map<Segment, CompletableFuture<Segment>> nextSegmentFutures = new ConcurrentHashMap<>();
  private ReentrantLock appendLock = new ReentrantLock();
  private Timer timer;
  private ScheduledFuture timerProcessTask;
  private List<AppendHook> appendHooks;

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
    partitionName = meta.getTopicId() + "@" + meta.getPartition();
    timer = new Timer(this);
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    return getLastSegment()
        .thenCompose(
            s -> {
              switch (s.getStatus()) {
                case WRITABLE:
                  return append0(s, new Segment.AppendContext(), batchRecord);
                case REPLICA_BREAK:
                  return nextSegment(s)
                      .thenCompose(n -> append0(n, new Segment.AppendContext(), batchRecord));
                case SEALED:
                  return CompletableFuture.completedFuture(new AppendResult(AppendStatus.FAIL));
              }
              throw new LinkyIOException(String.format("Unknown status %s", s.getStatus()));
            });
  }

  public CompletableFuture<AppendResult> append0(
      Segment segment, Segment.AppendContext segCtx, BatchRecord batchRecord) {
    BatchRecord.Builder batchRecordBuilder = batchRecord.toBuilder();
    int offsetCount = Utils.getOffsetCount(batchRecord);
    AppendContext ctx = new AppendContext();
    for (AppendHook hook : appendHooks) {
      hook.before(ctx, batchRecordBuilder);
    }
    if (log.isDebugEnabled()) {
      log.debug("[PARTITION_APPEND]{}", TextFormat.shortDebugString(batchRecord));
    }
    appendLock.lock();
    try {
      return segment
          .append(segCtx, batchRecordBuilder.build())
          .thenApply(
              appendResult -> {
                switch (appendResult.getStatus()) {
                  case SUCCESS:
                    Cursor cursor = new Cursor(appendResult.getIndex(), appendResult.getOffset());
                    ctx.setCursor(cursor);
                    ctx.setNextCursor(
                        new Cursor(
                            appendResult.getIndex(), appendResult.getOffset() + offsetCount));
                    for (AppendHook hook : appendHooks) {
                      hook.after(ctx);
                    }
                    return new AppendResult(cursor.toBytes());
                  default:
                    return new AppendResult(AppendStatus.FAIL);
                }
              });
    } finally {
      appendLock.unlock();
    }
  }

  @Override
  public byte[] getMeta(byte[] key) {
    try {
      GetKVResult getKVResult = getKV(key, true).get();
      return Optional.ofNullable(getKVResult)
          .map(rst -> rst.getBatchRecord())
          .map(
              r ->
                  r.getRecordsList().stream()
                      .filter(record -> Arrays.equals(record.getKey().toByteArray(), key))
                      .findAny()
                      .get()
                      .getValue()
                      .toByteArray())
          .orElse(null);
    } catch (Exception e) {
      e.printStackTrace();
      throw new LinkyIOException(e);
    }
  }

  @Override
  public CompletableFuture<Void> setMeta(byte[]... kv) {
    BatchRecord.Builder batchRecord = BatchRecord.newBuilder().setFlag(INVISIBLE_FLAG | META_FLAG);
    for (int i = 0; i < kv.length / 2; i++) {
      batchRecord.addRecords(
          Record.newBuilder()
              .setKey(ByteString.copyFrom(kv[i * 2]))
              .setValue(ByteString.copyFrom(kv[i * 2 + 1]))
              .build());
    }
    return append(batchRecord.build())
        .thenAccept(
            r -> {
              if (r.getStatus() != AppendStatus.SUCCESS) {
                throw new LinkyIOException(String.format("[META_SET_FAIL]%s", r));
              }
            });
  }

  private void startTimerService() {
    timer.init();
    timerProcessTask =
        TIMER_SCHEDULER.scheduleWithFixedDelay(
            () -> {
              try {
                timer.process();
              } catch (Throwable e) {
                e.printStackTrace();
              }
            },
            10,
            10,
            TimeUnit.MILLISECONDS);
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

  protected CompletableFuture<Segment> getLastWritableSegment() {
    return getLastSegment()
        .thenCompose(
            s -> {
              switch (s.getStatus()) {
                case WRITABLE:
                  return CompletableFuture.completedFuture(s);
                case REPLICA_BREAK:
                  return nextSegment(s).thenApply(n -> n);
                case SEALED:
                  return CompletableFuture.completedFuture(s);
              }
              throw new LinkyIOException(
                  String.format(
                      "%s getLastWritableSegment Unknown status %s", partitionName, s.getStatus()));
            });
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
                segments.put(s.getIndex(), s);
                nextSegmentFutures.get(lastSegment).complete(s);
                lastSegmentFuture = nextSegmentFutures.get(lastSegment);
                this.lastSegment = s;
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
    return get(cursor, true, true);
  }

  @Override
  public CompletableFuture<GetResult> get(byte[] cursor, boolean skipInvisible, boolean link) {
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
    long nextOffset = offset + 1;
    nextCursor.putLong(nextOffset);
    int finalSegmentIndex = segmentIndex;
    return recordFuture.thenCompose(
        r -> {
          if (r == null) {
            getResult.setStatus(GetStatus.NO_NEW_MSG);
            getResult.setNextCursor(cursor);
            return CompletableFuture.completedFuture(getResult);
          } else {
            if (link && (r.getFlag() & Constants.LINK_FLAG) != 0) {
              Cursor linkedCursor =
                  Cursor.get(
                      r.getRecords((int) (nextOffset - 1 - r.getFirstOffset()))
                          .getValue()
                          .toByteArray());
              return get(linkedCursor.toBytes(), false, true)
                  .thenApply(linkedGetResult -> linkedGetResult.setNextCursor(nextCursor.array()));
            }

            if (skipInvisible) {
              if ((r.getFlag() & INVISIBLE_FLAG) != 0) {
                return get(new Cursor(finalSegmentIndex, nextOffset).toBytes(), true, link);
              }
            }

            getResult.setBatchRecord(r);
            getResult.setNextCursor(nextCursor.array());
            return CompletableFuture.completedFuture(getResult);
          }
        });
  }

  @Override
  public void get(byte[] startCursor, byte[] endCursor, StreamObserver<BatchRecord> stream) {
    Cursor cursor = Cursor.get(startCursor);
    Cursor end = Cursor.get(endCursor);

    for (; cursor.compareTo(end) < 0; ) {
      try {
        GetResult rst = get(cursor.toBytes(), false, false).get();
        stream.onNext(rst.getBatchRecord());
        cursor = Cursor.get(rst.getNextCursor());
      } catch (Exception e) {
        // TODO: fixme
        e.printStackTrace();
      }
    }
    stream.onCompleted();
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

    timer.shutdown();
    if (timerProcessTask != null) {
      timerProcessTask.cancel(false);
    }
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

  @Override
  public String name() {
    return partitionName;
  }

  @Override
  public Cursor getNextCursor() {
    if (segments.size() == 0) {
      return new Cursor(0, 0);
    }
    Segment segment = segments.lastEntry().getValue();
    return new Cursor(segment.getIndex(), segment.getNextOffset());
  }

  public CompletableFuture<List<TimerIndex>> getTimerSlot(Cursor cursor) {
    CompletableFuture<List<TimerIndex>> future = new CompletableFuture<>();
    List<TimerIndex> timerIndexes = new LinkedList<>();
    getTimerSlot(cursor, timerIndexes)
        .thenApply(nil -> future.complete(timerIndexes))
        .exceptionally(t -> future.completeExceptionally(t));
    return future;
  }

  protected CompletableFuture<Void> getTimerSlot(Cursor cursor, List<TimerIndex> timerIndexes) {
    CompletableFuture<List<BatchRecord>> indexesFuture =
        segments.get(cursor.getIndex()).getTimerSlot(cursor.getOffset());
    return indexesFuture.thenCompose(
        records -> {
          if (records == null || records.size() == 0) {
            return CompletableFuture.completedFuture(null);
          }
          for (BatchRecord record : records) {
            timerIndexes.add(TimerUtils.transformTimerIndexRecord2TimerIndex(record));
          }
          Cursor prev = TimerUtils.getPreviousCursor(records.get(records.size() - 1));
          if (Cursor.NOOP.equals(prev)) {
            return CompletableFuture.completedFuture(null);
          }
          return getTimerSlot(prev, timerIndexes);
        });
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }

  @Override
  public AppendPipeline appendPipeline() {
    return new AppendPipelineImpl();
  }

  @Override
  public void registerAppendHook(AppendHook appendHook) {
    if (appendHooks == null) {
      appendHooks = new ArrayList<>();
    }
    appendHooks.add(appendHook);
  }

  class AppendPipelineImpl implements AppendPipeline {
    private CompletableFuture<Segment> segmentFuture = getLastWritableSegment();

    public synchronized CompletableFuture<AppendResult> append(
        Segment.AppendContext context, BatchRecord batchRecord) {
      return segmentFuture.thenCompose(s -> append0(s, context, batchRecord));
    }

    public synchronized CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
      return append(new Segment.AppendContext(), batchRecord);
    }
  }
}
