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

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class LocalSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(LocalSegment.class);
  private static final int MAIN = 1;
  private static final int FOLLOWER = 1 << 1;
  private int topic;
  private int partition;
  private int index;
  private long startOffset;
  private AtomicLong nextOffset;
  private int flag;
  private Role role;
  private List<StreamObserver<SegmentServiceProto.ReplicateRequest>> followerSenders =
      new ArrayList<>();
  private Map<String, AtomicLong> confirmOffsets = new ConcurrentHashMap<>();
  private volatile long confirmOffset = NO_OFFSET;
  private Queue<Waiting> waitConfirmRequests = new ConcurrentLinkedQueue<>();

  private long endOffset;
  private Map<Long, Long> indexMap;

  private WriteAheadLog wal;
  private BrokerContext brokerContext;

  public LocalSegment(SegmentMeta meta, WriteAheadLog wal, BrokerContext brokerContext) {
    this.topic = meta.getTopicId();
    this.partition = meta.getPartition();
    this.index = meta.getIndex();
    this.startOffset = meta.getStartOffset();
    this.nextOffset = new AtomicLong(this.startOffset);
    this.flag = meta.getFlag();
    //    this.role = (flag & MAIN) != 0 ? Role.MAIN : Role.FOLLOWER;

    this.wal = wal;

    this.brokerContext = brokerContext;

    indexMap = new ConcurrentHashMap<>();
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    CompletableFuture<AppendResult> rst = new CompletableFuture<>();
    try {
      int recordsCount = batchRecord.getRecordsCount();
      long offset = nextOffset.getAndAdd(recordsCount);
      batchRecord = BatchRecord.newBuilder(batchRecord).setFirstOffset(offset).build();

      CompletableFuture<WriteAheadLog.AppendResult> walFuture = wal.append(batchRecord);
      waitConfirmRequests.add(new Waiting(offset, rst));
      walFuture.thenAccept(
          r -> {
            this.confirmOffset = offset + recordsCount - 1;
            checkWaiting();
            indexMap.put(offset, r.getOffset());
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
    return null;
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    long physicalOffset = indexMap.get(offset);
    return wal.get(physicalOffset).handle((batchRecord, throwable) -> batchRecord);
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
  public CompletableFuture<Void> seal() {
    return null;
  }

  public void setWal(WriteAheadLog wal) {
    this.wal = wal;
  }

  enum Role {
    MAIN,
    FOLLOWER
  }

  private synchronized void checkWaiting() {
    for (; ; ) {
      Waiting waiting = waitConfirmRequests.peek();
      if (waiting == null) {
        return;
      }
      if (waiting.check()) {
        waitConfirmRequests.poll();
      }
    }
  }

  class Waiting {
    private final long offset;
    private final CompletableFuture<AppendResult> segmentAppendResult;

    public Waiting(long offset, CompletableFuture<AppendResult> segmentAppendResult) {
      this.offset = offset;
      this.segmentAppendResult = segmentAppendResult;
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
      segmentAppendResult.complete(new AppendResult(this.offset));
      return true;
    }
  }
}
