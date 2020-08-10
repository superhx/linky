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
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RemoteSegment implements Segment {
  private static final int READONLY_MARK = 1 << 1;
  private SegmentMeta meta;
  private long endOffset;
  private BrokerContext brokerContext;
  private DataNodeCnx dataNodeCnx;
  private String address;

  public RemoteSegment(SegmentMeta meta, BrokerContext brokerContext) {
    this.meta = meta;
    this.endOffset = meta.getEndOffset();
    this.brokerContext = brokerContext;
    this.dataNodeCnx = brokerContext.getDataNodeCnx();

    this.address = meta.getReplicas(0).getAddress();
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<ReplicateResult> replicate(BatchRecord batchRecord) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    CompletableFuture<BatchRecord> result = new CompletableFuture<>();
    dataNodeCnx
        .getSegmentServiceStub(address)
        .get(
            SegmentServiceProto.GetRecordRequest.newBuilder()
                .setTopicId(this.meta.getTopicId())
                .setPartition(this.meta.getPartition())
                .setIndex(this.meta.getIndex())
                .setOffset(offset)
                .build(),
            new StreamObserver<SegmentServiceProto.GetRecordResponse>() {
              @Override
              public void onNext(SegmentServiceProto.GetRecordResponse getRecordResponse) {
                result.complete(getRecordResponse.getBatchRecord());
              }

              @Override
              public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return result;
  }

  @Override
  public int getIndex() {
    return this.meta.getIndex();
  }

  @Override
  public long getStartOffset() {
    return this.meta.getStartOffset();
  }

  @Override
  public void setStartOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getEndOffset() {
    return this.endOffset;
  }

  @Override
  public void setEndOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SegmentMeta getMeta() {
    return null;
  }

  @Override
  public CompletableFuture<Void> seal() {
    return null;
  }

  @Override
  public CompletableFuture<Void> seal0() {
    if ((meta.getFlag() & READONLY_MARK) != 0) {
      return CompletableFuture.completedFuture(null);
    }
    List<Long> endOffsets = new LinkedList<>();
    return CompletableFuture.allOf(
            meta.getReplicasList().stream()
                .map(
                    r -> {
                      CompletableFuture<Void> rst = new CompletableFuture<>();
                      dataNodeCnx
                          .getSegmentServiceStub(r.getAddress())
                          .seal(
                              SegmentServiceProto.SealRequest.newBuilder().build(),
                              new StreamObserver<SegmentServiceProto.SealResponse>() {
                                @Override
                                public void onNext(SegmentServiceProto.SealResponse sealResponse) {
                                  endOffsets.add(sealResponse.getEndOffset());
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                  endOffsets.add(NO_OFFSET);
                                }

                                @Override
                                public void onCompleted() {
                                  rst.complete(null);
                                }
                              });
                      return rst;
                    })
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .thenApply(
            r -> {
              Collections.sort(endOffsets);
              this.endOffset = endOffsets.get(endOffsets.size() / 2) + 1;
              return null;
            });
  }
}
