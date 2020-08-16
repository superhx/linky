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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
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
  private List<StreamObserver<SegmentServiceProto.ReplicateRequest>> followerSenders =
      new ArrayList<>();
  private Map<String, AtomicLong> confirmOffsets = new ConcurrentHashMap<>();
  private volatile long confirmOffset = NO_OFFSET;
  private Queue<Waiting> waitConfirmRequests = new ConcurrentLinkedQueue<>();

  private long endOffset = NO_OFFSET;

  private WriteAheadLog wal;
  private BrokerContext brokerContext;
  private DataNodeCnx dataNodeCnx;
  private String storePath;
  private MappedFiles mappedFiles;
  private Set<String> failedReplicas = new CopyOnWriteArraySet<>();

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

    for (SegmentMeta.Replica replica : meta.getReplicasList()) {
      if (!brokerContext.getAddress().equals(replica.getAddress())) {
        continue;
      }
      this.role = (replica.getFlag() & FOLLOWER_MARK) == 0 ? Role.MAIN : Role.FOLLOWER;
      break;
    }

    initReplicator();
  }

  private void initReplicator() {
    if (this.role == Role.MAIN) {
      meta.getReplicasList().stream()
          .forEach(
              r -> {
                if (brokerContext.getAddress().equals(r.getAddress())) {
                  return;
                }
                AtomicLong followerConfirmOffset = new AtomicLong(NO_OFFSET);
                confirmOffsets.put(r.getAddress(), followerConfirmOffset);
                Context.current()
                    .fork()
                    .run(
                        () ->
                            followerSenders.add(
                                this.dataNodeCnx
                                    .getSegmentServiceStub(r.getAddress())
                                    .replicate(
                                        new StreamObserver<
                                            SegmentServiceProto.ReplicateResponse>() {
                                          @Override
                                          public void onNext(
                                              SegmentServiceProto.ReplicateResponse
                                                  replicateResponse) {
                                            followerConfirmOffset.set(
                                                replicateResponse.getConfirmOffset());
                                            checkWaiting();
                                          }

                                          @Override
                                          public void onError(Throwable throwable) {
                                            log.warn("replica segment {} fail", r.getAddress());
                                            failedReplicas.add(r.getAddress());
                                            status =
                                                failedReplicas.size()
                                                        < (meta.getReplicasList().size() + 1) / 2
                                                    ? Status.REPLICA_LOSS
                                                    : Status.REPLICA_BREAK;
                                            if (status == Status.REPLICA_BREAK) {
                                              checkWaiting();
                                              for (Waiting waiting : waitConfirmRequests) {
                                                waiting.future.complete(
                                                    new AppendResult(REPLICA_BREAK, NO_OFFSET));
                                              }
                                            }
                                          }

                                          @Override
                                          public void onCompleted() {}
                                        })));
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
      batchRecord = BatchRecord.newBuilder(batchRecord).setFirstOffset(offset).build();

      CompletableFuture<WriteAheadLog.AppendResult> walFuture = wal.append(batchRecord);
      waitConfirmRequests.add(new Waiting(offset, rst, new AppendResult(offset)));

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

      SegmentServiceProto.ReplicateRequest replicateRequest =
          SegmentServiceProto.ReplicateRequest.newBuilder()
              .setTopicId(topic)
              .setPartition(partition)
              .setIndex(index)
              .setBatchRecord(batchRecord)
              .build();
      for (StreamObserver<SegmentServiceProto.ReplicateRequest> follower : followerSenders) {
        follower.onNext(replicateRequest);
      }

      return rst;
    } catch (Throwable t) {
      log.error("append fail unexpect ex", t);
      rst.completeExceptionally(t);
      return rst;
    }
  }

  @Override
  public CompletableFuture<ReplicateResult> replicate(BatchRecord batchRecord) {
    CompletableFuture<ReplicateResult> rst = new CompletableFuture<>();
    if (this.status == Status.READONLY) {
      rst.completeExceptionally(new StoreException());
      return rst;
    }
    if (startOffset == NO_OFFSET) {
      setStartOffset(batchRecord.getFirstOffset());
    }
    log.info("replica {}", batchRecord);
    long replicaConfirmOffset = nextOffset.addAndGet(batchRecord.getRecordsCount()) - 1;

    waitConfirmRequests.add(
        new Waiting(replicaConfirmOffset, rst, new ReplicateResult(replicaConfirmOffset)));
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
    return dataNodeCnx
        .seal(
            SegmentManagerServiceProto.SealRequest.newBuilder()
                .setTopicId(topic)
                .setPartition(partition)
                .setIndex(index)
                .build())
        .thenAccept(o -> {});
  }

  @Override
  public CompletableFuture<Void> seal0() {
    this.status = Status.READONLY;
    log.info("start seal segment {}", meta);
    return CompletableFuture.allOf(
            waitConfirmRequests.stream()
                .map(w -> w.future)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .thenAccept(
            r -> {
              List<Long> offsets =
                  confirmOffsets.values().stream().map(o -> o.get()).collect(Collectors.toList());
              offsets.add(confirmOffset);
              Collections.sort(offsets);
              this.endOffset = offsets.get(offsets.size() / 2) + 1;
              this.meta.setEndOffset(endOffset);
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
      for (AtomicLong confirmOffset : confirmOffsets.values()) {
        if (confirmOffset.get() >= this.offset) {
          confirmCount++;
        }
      }
      if (confirmCount <= (confirmOffsets.size() + 1) / 2) {
        return false;
      }
      future.complete(result);
      return true;
    }
  }

  enum Role {
    MAIN,
    FOLLOWER
  }
}
