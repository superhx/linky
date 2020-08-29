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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Segment.AppendResult.Status.REPLICA_BREAK;

public class LocalSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(LocalSegment.class);
  private static final int fileSize = 12 * 1024 * 1024;
  private static final int INDEX_UNIT_SIZE = 12;
  private SegmentMeta.Builder meta;
  private int topic;
  private int partition;
  private int index;
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
  private Set<String> failedReplicas = new CopyOnWriteArraySet<>();
  boolean sinking = false;

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
                log.debug("{} scan pos {} return noop", mappedFile, lso);
                return lso;
              } else {
                log.debug("{} scan pos {} return index {}/{}", mappedFile, lso, offset, size);
              }
              return lso + 12;
            });

    this.meta = meta.toBuilder();
    this.topic = meta.getTopicId();
    this.partition = meta.getPartition();
    this.index = meta.getIndex();
    this.wal = wal;

    this.brokerContext = brokerContext;
    this.dataNodeCnx = brokerContext.getDataNodeCnx();
    this.setStartOffset(meta.getStartOffset());
    this.endOffset = meta.getEndOffset();
    this.confirmOffset = this.startOffset + mappedFiles.getConfirmOffset() / INDEX_UNIT_SIZE - 1;
    this.nextOffset.set(confirmOffset + 1);

    for (SegmentMeta.Replica replica : meta.getReplicasList()) {
      if (!brokerContext.getAddress().equals(replica.getAddress())) {
        continue;
      }
      this.role = (replica.getFlag() & FOLLOWER_MARK) == 0 ? Role.MAIN : Role.FOLLOWER;
      break;
    }

    initReplicator();

    scheduler.scheduleWithFixedDelay(
        () -> {
            if ((meta.getFlag() & SEAL_MARK) != 0) {
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
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
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

      CompletableFuture<WriteAheadLog.AppendResult> walFuture = wal.append(batchRecord);
      waitConfirmRequests.add(new Waiting(offset, rst, new AppendResult(offset)));

      SegmentServiceProto.ReplicateRequest replicateRequest =
          SegmentServiceProto.ReplicateRequest.newBuilder().setBatchRecord(batchRecord).build();
      for (Follower follower : followers) {
        follower.replicate(replicateRequest);
      }

      walFuture.thenAccept(
          r -> {
            this.confirmOffset = offset + recordsCount - 1;
            checkWaiting();
            ByteBuffer index = ByteBuffer.allocate(12);
            index.putLong(r.getOffset());
            index.putInt(r.getSize());
            index.flip();
            mappedFiles.write(index);
          });

      return rst;
    } catch (Throwable t) {
      log.error("append fail unexpect ex", t);
      rst.completeExceptionally(t);
      return rst;
    }
  }

  @Override
  public CompletableFuture<SegmentServiceProto.ReplicateResponse> replicate(
      SegmentServiceProto.ReplicateRequest request) {
    CompletableFuture<SegmentServiceProto.ReplicateResponse> rst = new CompletableFuture<>();
    if (this.status == Status.READONLY) {
      rst.completeExceptionally(new StoreException());
      return rst;
    }
    BatchRecord batchRecord = request.getBatchRecord();
    log.info("replica {}", batchRecord);

    if (nextOffset.get() != batchRecord.getFirstOffset()) {
      log.info("need reset to {}", nextOffset.get());
      return CompletableFuture.completedFuture(
          SegmentServiceProto.ReplicateResponse.newBuilder()
              .setStatus(SegmentServiceProto.ReplicateResponse.Status.RESET)
              .setConfirmOffset(nextOffset.get() - 1)
              .build());
    }

    if (startOffset == NO_OFFSET) {
      setStartOffset(batchRecord.getFirstOffset());
    }
    long replicaConfirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount()) - 1;

    waitConfirmRequests.add(
        new Waiting(
            replicaConfirmOffset,
            rst,
            SegmentServiceProto.ReplicateResponse.newBuilder()
                .setConfirmOffset(replicaConfirmOffset)
                .build()));
    wal.append(batchRecord)
        .thenAccept(
            r -> {
              this.confirmOffset = replicaConfirmOffset;
              checkWaiting();
              ByteBuffer index = ByteBuffer.allocate(12);
              index.putLong(r.getOffset());
              index.putInt(r.getSize());
              index.flip();
              mappedFiles.write(index);
            });
    return rst;
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
    this.meta.setFlag(this.meta.getFlag() & SEAL_MARK);
    dataNodeCnx
        .getSegmentServiceStub(request.getAddress())
        .sync(
            SegmentServiceProto.SyncRequest.newBuilder()
                .setTopicId(topic)
                .setPartition(partition)
                .setIndex(index)
                .setStartOffset(this.confirmOffset + 1)
                .build(),
            new StreamObserver<SegmentServiceProto.SyncResponse>() {
              @Override
              public void onNext(SegmentServiceProto.SyncResponse syncResponse) {
                BatchRecord batchRecord = syncResponse.getBatchRecord();
                if (nextOffset.get() != batchRecord.getFirstOffset()) {
                  log.info("sync sink not equal {}", batchRecord);
                  return;
                }
                log.info("sync sink {}", batchRecord);
                nextOffset.addAndGet(batchRecord.getRecordsCount());
                long replicaConfirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount()) - 1;
                wal.append(batchRecord)
                    .thenAccept(
                        r -> {
                          LocalSegment.this.confirmOffset = replicaConfirmOffset;
                          ByteBuffer index = ByteBuffer.allocate(12);
                          index.putLong(r.getOffset());
                          index.putInt(r.getSize());
                          index.flip();
                          mappedFiles.write(index);
                        });
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
        offset <= LocalSegment.this.confirmOffset;
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
        .thenAccept(n -> {
          responseObserver.onCompleted();
        })
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
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
    this.confirmOffset = this.startOffset - 1;
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
            waitConfirmRequests.stream()
                .map(w -> w.future)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .thenAccept(
            r -> {
              this.endOffset = this.confirmOffset + 1;
              this.meta.setEndOffset(endOffset);
              this.meta.setFlag(this.meta.getFlag() & SEAL_MARK);
              try {
                Utils.str2file(
                    JsonFormat.printer().print(this.meta),
                    Utils.getSegmentMetaPath(
                        this.brokerContext.getStorePath(),
                        meta.getTopicId(),
                        meta.getPartition(),
                        meta.getIndex()));
              } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
              }
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

  class Follower implements StreamObserver<SegmentServiceProto.ReplicateResponse> {
    private long expectedNextOffset;
    private long confirmOffset = NO_OFFSET;
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
      log.info("replicate {} {}", followerAddress, request);
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
      failedReplicas.add(this.followerAddress);
      status =
          failedReplicas.size() < (meta.getReplicasList().size() + 1) / 2
              ? Status.REPLICA_LOSS
              : Status.REPLICA_BREAK;
      if (status == Status.REPLICA_BREAK) {
        checkWaiting();
        for (Waiting waiting : waitConfirmRequests) {
          waiting.future.complete(new AppendResult(REPLICA_BREAK, NO_OFFSET));
        }
      }
    }

    @Override
    public void onCompleted() {}

    public long getConfirmOffset() {
      return confirmOffset;
    }

    public boolean isBroken() {
      return broken;
    }
  }
}
