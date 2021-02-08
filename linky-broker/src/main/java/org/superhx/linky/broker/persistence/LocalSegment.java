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
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.ChunkMeta;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.List;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Segment.AppendResult.Status.REPLICA_BREAK;
import static org.superhx.linky.broker.persistence.Segment.AppendResult.Status.TERM_EXPIRED;

public class LocalSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(LocalSegment.class);
  private SegmentMeta.Builder meta;
  private final int topicId;
  private final int partition;
  private final int index;
  private final String segmentId;
  private volatile AtomicInteger term = new AtomicInteger();
  private AtomicLong nextOffset = new AtomicLong();
  private Status status = Status.WRITABLE;
  private Role role;
  private List<Follower> followers = new CopyOnWriteArrayList<>();

  private volatile long confirmOffset;
  private volatile long commitOffset;
  private long endOffset;

  private Queue<Waiting> waitConfirmRequests = new ConcurrentLinkedQueue<>();

  private BrokerContext brokerContext;
  private DataNodeCnx dataNodeCnx;
  private ScheduledFuture<?> followerScanner;
  private Chunk lastChunk;
  private NavigableMap<Long, Chunk> chunks = new ConcurrentSkipListMap<>();

  private static final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor();

  public LocalSegment(
      SegmentMeta meta,
      BrokerContext brokerContext,
      DataNodeCnx dataNodeCnx,
      ChunkManager chunkManager) {
    this.topicId = meta.getTopicId();
    this.partition = meta.getPartition();
    this.index = meta.getIndex();
    this.segmentId = String.format("%s@%s@%s", topicId, partition, index);

    this.brokerContext = brokerContext;
    this.dataNodeCnx = dataNodeCnx;
    this.endOffset = meta.getEndOffset();

    List<Chunk> chunks = chunkManager.getChunks(topicId, partition, index);
    if (chunks.size() == 0) {
      lastChunk =
          chunkManager.newChunk(
              ChunkMeta.newBuilder()
                  .setTopicId(topicId)
                  .setPartition(partition)
                  .setSegmentIndex(index)
                  .setStartOffset(0)
                  .build());
      this.chunks.put(0L, lastChunk);
    }
    for (Chunk chunk : chunks) {
      this.chunks.put(chunk.startOffset(), chunk);
    }
    lastChunk = this.chunks.lastEntry().getValue();

    this.confirmOffset = lastChunk.getConfirmOffset();
    this.nextOffset.set(confirmOffset);

    updateMeta(meta);
    followerScanner =
        scheduler.scheduleWithFixedDelay(() -> checkFollowers(), 30, 30, TimeUnit.SECONDS);
  }

  @Override
  public void updateMeta(SegmentMeta meta) {
    int currentTerm = term.get();
    if (currentTerm > meta.getTerm()) {
      log.warn("[SEGMENT_META_UPDATE_FAIL]{},{},{}", segmentId, this.meta, meta);
      return;
    }
    this.meta = meta.toBuilder();
    term.compareAndSet(currentTerm, meta.getTerm());

    for (SegmentMeta.Replica replica : meta.getReplicasList()) {
      if (!brokerContext.getAddress().equals(replica.getAddress())) {
        continue;
      }
      this.role = (replica.getFlag() & FOLLOWER_MARK) == 0 ? Role.MAIN : Role.FOLLOWER;
      break;
    }
    if (role == Role.FOLLOWER) {
        this.meta.clearReplicas();
    }
    Context.current().fork().run(() -> checkFollowers());
  }

  @Override
  public void init() {
    for (Chunk chunk : chunks.values()) {
      chunk.init();
    }
  }

  @Override
  public void start() {
    for (Chunk chunk : chunks.values()) {
      chunk.start();
    }
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
    for (Chunk chunk : chunks.values()) {
      chunk.shutdown();
    }
  }

  @Override
  public synchronized CompletableFuture<AppendResult> append(
      AppendContext ctx, BatchRecord batchRecord) {
    CompletableFuture<AppendResult> rst = new CompletableFuture<>();
    if (ctx.getTerm() != term.get()) {
      return CompletableFuture.completedFuture(new AppendResult(TERM_EXPIRED, NO_OFFSET));
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
              .setSegmentIndex(index)
              .setFirstOffset(offset)
              .setStoreTimestamp(System.currentTimeMillis())
              .setTerm(ctx.getTerm())
              .build();

      CompletableFuture<Void> localWriteFuture = getLastChunk().append(batchRecord);
      waitConfirmRequests.add(new Waiting(offset, rst, new AppendResult(offset)));

      SegmentServiceProto.ReplicateRequest replicateRequest =
          SegmentServiceProto.ReplicateRequest.newBuilder()
              .setBatchRecord(batchRecord)
              .setCommitOffset(commitOffset)
              .build();
      for (Follower follower : followers) {
        follower.replicate(replicateRequest);
      }

      localWriteFuture.thenAccept(
          r -> {
            this.confirmOffset = offset + recordsCount;
            checkWaiting();
          });
      return rst.thenApply(
          r -> {
            switch (r.getStatus()) {
              case SUCCESS:
              case REPLICA_LOSS:
                commitOffset = r.getOffset() + recordsCount;
            }
            return r;
          });
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
    BatchRecord batchRecord = request.getBatchRecord();
    if (log.isDebugEnabled()) {
      log.debug("{} receive replica {}", segmentId, batchRecord);
    }
    int currentTerm = term.get();
    if (batchRecord.getTerm() < currentTerm) {
      responseObserver.onNext(
          SegmentServiceProto.ReplicateResponse.newBuilder()
              .setStatus(SegmentServiceProto.ReplicateResponse.Status.EXPIRED)
              .build());
      return;
    }

    if (nextOffset.get() != batchRecord.getFirstOffset()) {
      log.info("{} main replicate need reset to {}", segmentId, nextOffset.get());
      responseObserver.onNext(
          SegmentServiceProto.ReplicateResponse.newBuilder()
              .setStatus(SegmentServiceProto.ReplicateResponse.Status.RESET)
              .setWriteOffset(nextOffset.get())
              .setConfirmOffset(confirmOffset)
              .build());
      return;
    }

    if (batchRecord.getTerm() > currentTerm) {
      log.info(
          "{} update term {} and reset to {}", segmentId, batchRecord.getTerm(), nextOffset.get());
      nextOffset.set(commitOffset);
      confirmOffset = commitOffset;
      responseObserver.onNext(
          SegmentServiceProto.ReplicateResponse.newBuilder()
              .setStatus(SegmentServiceProto.ReplicateResponse.Status.RESET)
              .setWriteOffset(commitOffset)
              .setConfirmOffset(commitOffset)
              .build());
      if (!term.compareAndSet(currentTerm, batchRecord.getTerm())) {
        log.warn("{} expect term {} but {}", currentTerm, term.get());
        responseObserver.onNext(
            SegmentServiceProto.ReplicateResponse.newBuilder()
                .setStatus(SegmentServiceProto.ReplicateResponse.Status.EXPIRED)
                .build());
        return;
      }
      return;
    }

    commitOffset = request.getCommitOffset();
    long replicaConfirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount());
    getLastChunk()
        .append(batchRecord)
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
    return getChunk(offset).get(offset);
  }

  @Override
  public int getIndex() {
    return index;
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
  public CompletableFuture<Void> reclaimSpace(long offset) {
    log.info("[RECLAIM] {} to {}", segmentId, offset);
    // TODO: reclaim space
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public SegmentMeta getMeta() {
    SegmentMeta.Builder builder =
        this.meta.clone().setEndOffset(endOffset).setTerm(term.get()).clearReplicas();
    for (SegmentMeta.Replica replica : meta.getReplicasList()) {
      if (replica.getAddress().equals(brokerContext.getAddress())) {
        builder.addReplicas(replica.toBuilder().setReplicaOffset(confirmOffset));
      } else if (role == Role.MAIN) {
        builder.addReplicas(replica.toBuilder().setReplicaOffset(commitOffset));
      }
    }
    return builder.build();
  }

  @Override
  public CompletableFuture<Void> seal() {
    term.compareAndSet(0, 1);
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
                      this.brokerContext.getStorePath(), topicId, partition, index));
              log.info("complete seal segment {} endOffset {}", meta, this.endOffset);
            });
  }

  protected void checkFollowers() {
    Set<String> newFollowers =
        meta.getReplicasList().stream()
            .map(r -> r.getAddress())
            .filter(addr -> !addr.equals(brokerContext.getAddress()))
            .collect(Collectors.toSet());
    Set<String> oldFollowers =
        followers.stream().map(f -> f.getAddress()).collect(Collectors.toSet());
    for (Follower follower : followers) {
      if (newFollowers.contains(follower.getAddress())) {
        continue;
      }
      log.info("[SEGMENT_FOLLOWER_REMOVE]{},{}", segmentId, follower);
      follower.shutdown();
      followers.remove(follower);
    }

    for (String newFollower : newFollowers) {
      if (!oldFollowers.contains(newFollower)) {
        log.info("[SEGMENT_FOLLOWER_ADD]{},{}", segmentId, newFollower);
        followers.add(new Follower(newFollower));
      }
    }
    for (Follower follower : followers) {
      if (!follower.isBroken()) {
        continue;
      }
      followers.add(new Follower(follower.followerAddress));
      followers.remove(follower);
    }
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

  private Chunk getLastChunk() {
    return lastChunk;
  }

  private Chunk getChunk(long offset) {
    if (lastChunk.startOffset() <= offset) {
      return lastChunk;
    }
    return chunks.floorEntry(offset).getValue();
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
    private long expectedNextOffset = 0;
    private long confirmOffset = NO_OFFSET;
    private long writeOffset = NO_OFFSET;
    private String followerAddress;
    private StreamObserver<SegmentServiceProto.ReplicateRequest> follower;
    private volatile boolean broken = false;
    Future<?> catchUpTask;

    public Follower(String followerAddress) {
      this.followerAddress = followerAddress;
      this.follower = dataNodeCnx.getSegmentServiceStub(followerAddress).replicate(this);
      replicate(
          SegmentServiceProto.ReplicateRequest.newBuilder()
              .setBatchRecord(
                  BatchRecord.newBuilder()
                      .setTopicId(topicId)
                      .setPartition(partition)
                      .setSegmentIndex(index)
                      .setFirstOffset(NO_OFFSET)
                      .build())
              .build());
    }

    public String getAddress() {
      return followerAddress;
    }

    @Override
    public void shutdown() {
      if (follower != null) {
        follower.onCompleted();
      }
    }

    // TODO: thread safe refactor & catch up async/traffic control
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
        expectedNextOffset = replicateResponse.getConfirmOffset();
        log.info("[SEGMENT_REPLICATE_RESET]{},{}", segmentId, expectedNextOffset);
        catchup();
      }
      if (replicateResponse.getStatus() == SegmentServiceProto.ReplicateResponse.Status.EXPIRED) {
        log.info("[SEGMENT_REPLICATE_EXPIRED]{}", segmentId);
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
