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

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.superhx.linky.broker.persistence.Segment.AppendResult.Status.REPLICA_BREAK;

public class LocalSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(LocalSegment.class);
  private static final int fileSize = 12 * 1024;
  private static final int INDEX_UNIT_SIZE = 12;
  private SegmentMeta.Builder meta;
  private int topic;
  private int partition;
  private int index;
  private final String segmentId;
  private long startOffset = NO_OFFSET;
  private AtomicLong nextOffset;
  private Status status = Status.WRITABLE;
  private Role role;
  private List<Follower> followers = new CopyOnWriteArrayList<>();

  private volatile long confirmOffset = NO_OFFSET;

  private Queue<Waiting> waitConfirmRequests = new ConcurrentLinkedQueue<>();

  private long endOffset = NO_OFFSET;

  private WriteAheadLog wal;
  private BrokerContext brokerContext;
  private DataNodeCnx dataNodeCnx;
  private String storePath;
  private MappedFiles mappedFiles;
  boolean sinking = false;
  private ScheduledFuture<?> followerScanner;

  private static final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor();

  public LocalSegment(SegmentMeta meta, WriteAheadLog wal, BrokerContext brokerContext) {
    this.storePath =
        String.format(
            "%s/segments/%s/%s/%s",
            brokerContext.getStorePath(), meta.getTopicId(), meta.getPartition(), meta.getIndex());
    mappedFiles =
        new MappedFiles(
            this.storePath,
            fileSize,
            (mappedFile, lso) -> {
              ByteBuffer index = mappedFile.read(lso, INDEX_UNIT_SIZE);
              long offset = index.getLong();
              int size = index.getInt();
              if (size == 0) {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return noop", mappedFile, lso);
                }
                return lso;
              } else {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return index {}/{}", mappedFile, lso, offset, size);
                }
              }
              return lso + 12;
            },
            null);

    this.meta = meta.toBuilder();
    this.topic = meta.getTopicId();
    this.partition = meta.getPartition();
    this.index = meta.getIndex();
    this.segmentId =
        String.format("%s-%s-%s", meta.getTopicId(), meta.getPartition(), meta.getIndex());
    this.wal = wal;

    this.brokerContext = brokerContext;
    this.dataNodeCnx = brokerContext.getDataNodeCnx();
    this.setStartOffset(meta.getStartOffset());
    this.endOffset = meta.getEndOffset();
    this.confirmOffset = this.startOffset + mappedFiles.getConfirmOffset() / INDEX_UNIT_SIZE;
    this.nextOffset.set(confirmOffset);

    for (SegmentMeta.Replica replica : meta.getReplicasList()) {
      if (!brokerContext.getAddress().equals(replica.getAddress())) {
        continue;
      }
      this.role = (replica.getFlag() & FOLLOWER_MARK) == 0 ? Role.MAIN : Role.FOLLOWER;
      break;
    }

    initReplicator();

    followerScanner =
        scheduler.scheduleWithFixedDelay(
            () -> {
              if ((meta.getFlag() & SEAL_MARK) != 0) {
                return;
              }
              if (status != Status.WRITABLE) {
                return;
              }
              for (Follower follower : followers) {
                if (!follower.isBroken()) {
                  continue;
                }
                followers.add(new Follower(follower.followerAddress, NO_OFFSET));
                followers.remove(follower);
              }
            },
            30,
            30,
            TimeUnit.SECONDS);
  }

  @Override
  public void init() {
    mappedFiles.init();
  }

  @Override
  public void start() {
    mappedFiles.start();
  }

  @Override
  public void shutdown() {
    if (followerScanner != null) {
      followerScanner.cancel(false);
      try {
        followerScanner.get();
      } catch (Exception e) {
      }
    }
    for (Follower follower : followers) {
      follower.shutdown();
    }
    mappedFiles.shutdown();
  }

  private void initReplicator() {
    if (this.role == Role.MAIN && (this.meta.getFlag() & SEAL_MARK) == 0) {
      meta.getReplicasList().stream()
          .forEach(
              r -> {
                if (brokerContext.getAddress().equals(r.getAddress())) {
                  return;
                }
                Context.current()
                    .fork()
                    .run(
                        () -> {
                          followers.add(new Follower(r.getAddress(), meta.getStartOffset()));
                        });
              });
    }
  }

  @Override
  public synchronized CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    CompletableFuture<AppendResult> rst = new CompletableFuture<>();
    if (this.status == Status.READONLY) {
      rst.completeExceptionally(new StoreException());
      return rst;
    }
    if (this.status == Status.REPLICA_BREAK) {
      return CompletableFuture.completedFuture(new AppendResult(REPLICA_BREAK, NO_OFFSET));
    }
    try {
      int recordsCount = batchRecord.getRecordsCount();
      long offset = nextOffset.getAndAdd(recordsCount);
      batchRecord =
          BatchRecord.newBuilder(batchRecord)
              .setTopicId(meta.getTopicId())
              .setFirstOffset(offset)
              .setSegmentIndex(index)
              .build();

      WriteAheadLog.AppendResult walFuture = wal.append(batchRecord);
      waitConfirmRequests.add(new Waiting(offset, rst, new AppendResult(offset)));

      SegmentServiceProto.ReplicateRequest replicateRequest =
          SegmentServiceProto.ReplicateRequest.newBuilder().setBatchRecord(batchRecord).build();
      for (Follower follower : followers) {
        follower.replicate(replicateRequest);
      }

      walFuture.thenAccept(
          r -> {
            this.confirmOffset = offset + recordsCount;
            checkWaiting();
          });
      return rst;
    } catch (Throwable t) {
      log.error("append fail unexpected ex", t);
      rst.completeExceptionally(t);
      return rst;
    }
  }

  @Override
  public synchronized void replicate(
      SegmentServiceProto.ReplicateRequest request,
      StreamObserver<SegmentServiceProto.ReplicateResponse> responseObserver) {
    CompletableFuture<SegmentServiceProto.ReplicateResponse> rst = new CompletableFuture<>();
    if (this.status == Status.READONLY) {
      rst.completeExceptionally(new StoreException());
      responseObserver.onError(new LinkyIOException("Segment READONLY"));
      return;
    }
    BatchRecord batchRecord = request.getBatchRecord();
    if (log.isDebugEnabled()) {
      log.debug("receive replica {}", batchRecord);
    }

    if (nextOffset.get() != batchRecord.getFirstOffset()) {
      log.info("need reset to {}", nextOffset.get());
      responseObserver.onNext(
          SegmentServiceProto.ReplicateResponse.newBuilder()
              .setStatus(SegmentServiceProto.ReplicateResponse.Status.RESET)
              .setWriteOffset(nextOffset.get())
              .setConfirmOffset(confirmOffset)
              .build());
      return;
    }

    if (startOffset == NO_OFFSET) {
      setStartOffset(batchRecord.getFirstOffset());
    }
    long replicaConfirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount());

    wal.append(batchRecord)
        .thenAccept(
            r -> {
              this.confirmOffset = replicaConfirmOffset;
              replicateResponse(responseObserver);
            });
    replicateResponse(responseObserver);
  }

  protected synchronized void replicateResponse(
      StreamObserver<SegmentServiceProto.ReplicateResponse> responseObserver) {
    responseObserver.onNext(
        SegmentServiceProto.ReplicateResponse.newBuilder()
            .setConfirmOffset(confirmOffset)
            .setWriteOffset(nextOffset.get())
            .build());
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    return mappedFiles
        .read((offset - this.startOffset) * 12, 12)
        .thenCompose(
            index -> {
              long physicalOffset = index.getLong();
              int size = index.getInt();
              return wal.get(physicalOffset, size);
            });
  }

  @Override
  public void putIndex(Index index) {
    long expectNextOffset = this.startOffset + mappedFiles.getWriteOffset() / INDEX_UNIT_SIZE;
    if (expectNextOffset != index.getOffset()) {
      log.warn("{} {} not match expect nextOffset {}", segmentId, index, expectNextOffset);
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("put {} {}", segmentId, index);
    }
    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.putLong(index.getPhysicalOffset());
    buf.putInt(index.getSize());
    buf.flip();
    mappedFiles.append(buf);
  }

  @Override
  public synchronized void syncCmd(
      SegmentServiceProto.SyncCmdRequest request,
      StreamObserver<SegmentServiceProto.SyncCmdResponse> responseObserver) {
    if (sinking == true) {
      responseObserver.onNext(SegmentServiceProto.SyncCmdResponse.newBuilder().build());
      responseObserver.onCompleted();
      return;
    }
    sinking = true;
    this.status = Status.READONLY;
    this.meta.setFlag(this.meta.getFlag() | SEAL_MARK);
    dataNodeCnx
        .getSegmentServiceStub(request.getAddress())
        .sync(
            SegmentServiceProto.SyncRequest.newBuilder()
                .setTopicId(topic)
                .setPartition(partition)
                .setIndex(index)
                .setStartOffset(nextOffset.get())
                .build(),
            new StreamObserver<SegmentServiceProto.SyncResponse>() {
              @Override
              public void onNext(SegmentServiceProto.SyncResponse syncResponse) {
                BatchRecord batchRecord = syncResponse.getBatchRecord();
                if (nextOffset.get() != batchRecord.getFirstOffset()) {
                  log.info("sync sink not equal expect {} but {}", nextOffset.get(), batchRecord);
                  log.info("sync sink not equal {}", batchRecord);
                  responseObserver.onError(
                      new LinkyIOException(
                          String.format(
                              "sync sink not match expect %s but %s",
                              nextOffset.get(), batchRecord)));
                  return;
                }
                log.info("sync sink {}", batchRecord);
                long confirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount());
                wal.append(batchRecord)
                    .thenAccept(r -> LocalSegment.this.confirmOffset = confirmOffset);
              }

              @Override
              public void onError(Throwable throwable) {
                log.info("sink fail", throwable);
                sinking = false;
                responseObserver.onError(throwable);
              }

              @Override
              public void onCompleted() {
                sinking = false;
                responseObserver.onNext(SegmentServiceProto.SyncCmdResponse.newBuilder().build());
                responseObserver.onCompleted();
              }
            });
  }

  @Override
  public void sync(
      SegmentServiceProto.SyncRequest request,
      StreamObserver<SegmentServiceProto.SyncResponse> responseObserver) {
    CompletableFuture<Void> lastReplicator = CompletableFuture.completedFuture(null);
    for (long offset = request.getStartOffset();
        offset < LocalSegment.this.confirmOffset;
        offset++) {
      long finalOffset = offset;
      lastReplicator =
          lastReplicator.thenAccept(
              n ->
                  get(finalOffset)
                      .thenAccept(
                          r ->
                              responseObserver.onNext(
                                  SegmentServiceProto.SyncResponse.newBuilder()
                                      .setBatchRecord(r)
                                      .build())));
    }
    lastReplicator
        .thenAccept(n -> responseObserver.onCompleted())
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void forceIndex() {
    mappedFiles.force();
  }

  @Override
  public void truncateDirtyIndex(long physicalOffset) {
    try {
      for (long offset = mappedFiles.getConfirmOffset(); offset > 0; offset = offset - 12) {
        ByteBuffer index = mappedFiles.read(offset - INDEX_UNIT_SIZE, 12).get();
        long indexPhysicalOffset = index.getLong();
      }
    } catch (Exception e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public void setStartOffset(long offset) {
    this.startOffset = offset;
    this.nextOffset = new AtomicLong(this.startOffset);
    this.confirmOffset = this.startOffset;
  }

  @Override
  public long getEndOffset() {
    return this.endOffset;
  }

  @Override
  public void setEndOffset(long offset) {
    this.endOffset = offset;
  }

  @Override
  public SegmentMeta getMeta() {
    return this.meta
        .clone()
        .setStartOffset(this.startOffset)
        .setEndOffset(this.endOffset)
        .clearReplicas()
        .addReplicas(
            SegmentMeta.Replica.newBuilder()
                .setReplicaOffset(this.confirmOffset)
                .setAddress(this.brokerContext.getAddress())
                .build())
        .build();
  }

  @Override
  public CompletableFuture<Void> seal() {
    return seal0()
        .thenCompose(
            n ->
                dataNodeCnx.seal(
                    SegmentManagerServiceProto.SealRequest.newBuilder()
                        .setTopicId(topic)
                        .setPartition(partition)
                        .setIndex(index)
                        .build()))
        .thenAccept(l -> {});
  }

  @Override
  public synchronized CompletableFuture<Void> seal0() {
    if (this.status == Status.READONLY) {
      return CompletableFuture.completedFuture(null);
    }
    this.status = Status.READONLY;
    log.info("start seal segment {}", meta);
    return CompletableFuture.allOf(
            waitConfirmRequests.stream().map(w -> w.future).toArray(CompletableFuture[]::new))
        .thenAccept(
            r -> {
              this.endOffset = this.confirmOffset;
              this.meta.setEndOffset(endOffset);
              this.meta.setFlag(this.meta.getFlag() | SEAL_MARK);
              Utils.byte2file(
                  Utils.pb2jsonBytes(this.meta),
                  Utils.getSegmentMetaPath(
                      this.brokerContext.getStorePath(), topic, partition, index));
              log.info("complete seal segment {} endOffset {}", meta, this.endOffset);
            });
  }

  private synchronized void checkWaiting() {
    for (; ; ) {
      Waiting waiting = waitConfirmRequests.peek();
      if (waiting == null) {
        return;
      }
      if (waiting.check()) {
        waitConfirmRequests.poll();
      } else {
        return;
      }
    }
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return meta.toString();
  }

  class Waiting<T> {
    private final long offset;
    private final CompletableFuture<T> future;
    private final T result;

    public Waiting(long offset, CompletableFuture<T> future, T result) {
      this.offset = offset;
      this.future = future;
      this.result = result;
    }

    public boolean check() {
      int confirmCount = 0;
      if (confirmOffset >= this.offset) {
        confirmCount++;
      }
      if (role == Role.MAIN) {
        for (Follower follower : followers) {
          if (follower.getConfirmOffset() >= this.offset) {
            confirmCount++;
          }
        }
        if (confirmCount < (meta.getReplicaNum() / 2 + 1)) {
          return false;
        }
      }
      future.complete(result);
      return true;
    }
  }

  enum Role {
    MAIN,
    FOLLOWER
  }

  class Follower implements StreamObserver<SegmentServiceProto.ReplicateResponse>, Lifecycle {
    private long expectedNextOffset;
    private long confirmOffset = NO_OFFSET;
    private long writeOffset = NO_OFFSET;
    private String followerAddress;
    private StreamObserver<SegmentServiceProto.ReplicateRequest> follower;
    private volatile boolean broken = false;
    Future<?> catchUpTask;

    public Follower(String followerAddress, long startOffset) {
      this.followerAddress = followerAddress;
      this.expectedNextOffset = startOffset;
      this.follower = dataNodeCnx.getSegmentServiceStub(followerAddress).replicate(this);
      if (startOffset == NO_OFFSET) {
        replicate(
            SegmentServiceProto.ReplicateRequest.newBuilder()
                .setBatchRecord(
                    BatchRecord.newBuilder()
                        .setTopicId(topic)
                        .setPartition(partition)
                        .setSegmentIndex(index)
                        .setFirstOffset(NO_OFFSET)
                        .build())
                .build());
      }
    }

    @Override
    public void shutdown() {
      if (follower != null) {
        follower.onCompleted();
      }
    }

    public void replicate(SegmentServiceProto.ReplicateRequest request) {
      if (broken == true) {
        return;
      }
      if (expectedNextOffset != request.getBatchRecord().getFirstOffset()) {
        if (expectedNextOffset != NO_OFFSET) {
          catchup();
        }
        return;
      }
      if (log.isDebugEnabled()) {
        log.debug("send replica to {} {}", followerAddress, request);
      }
      follower.onNext(request);
      expectedNextOffset += request.getBatchRecord().getRecordsCount();
    }

    public synchronized void catchup() {
      if (catchUpTask == null) {
        catchUpTask =
            scheduler.submit(
                () -> {
                  CompletableFuture<Void> lastReplicator = null;
                  for (long offset = expectedNextOffset;
                      offset <= LocalSegment.this.confirmOffset;
                      offset++) {
                    long finalOffset = offset;
                    if (lastReplicator == null) {
                      lastReplicator =
                          get(finalOffset)
                              .thenAccept(
                                  r ->
                                      replicate(
                                          SegmentServiceProto.ReplicateRequest.newBuilder()
                                              .setBatchRecord(r)
                                              .build()));
                    } else {
                      lastReplicator =
                          lastReplicator.thenAccept(
                              n ->
                                  get(finalOffset)
                                      .thenAccept(
                                          r ->
                                              replicate(
                                                  SegmentServiceProto.ReplicateRequest.newBuilder()
                                                      .setBatchRecord(r)
                                                      .build())));
                    }
                  }
                  lastReplicator.thenAccept(n -> catchUpTask = null);
                });
      }
    }

    @Override
    public void onNext(SegmentServiceProto.ReplicateResponse replicateResponse) {
      if (replicateResponse.getStatus() == SegmentServiceProto.ReplicateResponse.Status.SUCCESS) {
        confirmOffset = replicateResponse.getConfirmOffset();
        writeOffset = replicateResponse.getWriteOffset();
        checkWaiting();
      }
      if (replicateResponse.getStatus() == SegmentServiceProto.ReplicateResponse.Status.RESET) {
        expectedNextOffset = replicateResponse.getConfirmOffset() + 1;
        log.info("reset expectedNextOffset to {}", expectedNextOffset);
        catchup();
      }
    }

    @Override
    public void onError(Throwable throwable) {
      if (broken) {
        return;
      }
      broken = true;
      log.warn("replica segment {} fail", followerAddress, throwable);
      handleFollowerFail();
    }

    @Override
    public void onCompleted() {}

    public long getConfirmOffset() {
      return confirmOffset;
    }

    public long getWriteOffset() {
      return writeOffset;
    }

    public boolean isBroken() {
      return broken;
    }
  }

  protected synchronized void handleFollowerFail() {
    int normal = 1;
    for (Follower follower : followers) {
      if (!follower.isBroken()) {
        normal++;
      }
    }
    if (normal < meta.getReplicaNum() / 2 + 1) {
      status = Status.REPLICA_BREAK;
      checkWaiting();
      for (Waiting waiting : waitConfirmRequests) {
        waiting.future.complete(new AppendResult(REPLICA_BREAK, NO_OFFSET));
      }
    }
  }
}
