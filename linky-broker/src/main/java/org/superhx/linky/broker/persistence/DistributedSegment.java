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
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DistributedSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(DistributedSegment.class);
  private SegmentMeta.Builder meta;
  private DataNodeCnx dataNodeCnx;
  private int addressIndex;
  private String address;
  private Segment localSegment;

  public DistributedSegment(SegmentMeta meta, Segment localSegment, DataNodeCnx dataNodeCnx) {
    updateMeta(meta);
    this.dataNodeCnx = dataNodeCnx;
    this.localSegment = localSegment;
    log.info("open distributed segment {}", meta);
  }

  @Override
  public CompletableFuture<AppendResult> append(AppendContext ctx, BatchRecord batchRecord) {
    ctx.setTerm(0);
    if (localSegment == null) {
      throw new UnsupportedOperationException();
    }
    return localSegment.append(ctx, batchRecord);
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    if (this.localSegment != null) {
      return this.localSegment.get(offset);
    }
    CompletableFuture<BatchRecord> result = new CompletableFuture<>();
    get0(offset)
        .thenAccept(r -> result.complete(r))
        .exceptionally(
            t1 -> {
              get0(offset)
                  .thenAccept(r -> result.complete(r))
                  .exceptionally(
                      t2 -> {
                        result.completeExceptionally(t2);
                        return null;
                      });
              return null;
            });
    return result;
  }

  public CompletableFuture<BatchRecord> get0(long offset) {
    CompletableFuture<BatchRecord> result = new CompletableFuture<>();
    String aliveNode = getAliveNode();
    dataNodeCnx
        .getSegmentServiceStub(aliveNode)
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
                markReqNodeFail(address);
                result.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return result;
  }

  protected String getAliveNode() {
    return address;
  }

  protected void markReqNodeFail(String address) {
    addressIndex++;
    this.address = meta.getReplicas(addressIndex % meta.getReplicasList().size()).getAddress();
  }

  @Override
  public int getIndex() {
    return this.meta.getIndex();
  }

  @Override
  public long getEndOffset() {
    return this.meta.getEndOffset();
  }

  @Override
  public void setEndOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> reclaimSpace(long offset) {
    SegmentServiceProto.ReclaimRequest request =
        SegmentServiceProto.ReclaimRequest.newBuilder()
            .setTopicId(meta.getTopicId())
            .setPartition(meta.getPartition())
            .setIndex(meta.getIndex())
            .setOffset(offset)
            .build();
    return CompletableFuture.allOf(
        meta.getReplicasList().stream()
            .map(r -> dataNodeCnx.reclaim(r.getAddress(), request))
            .collect(Collectors.toList())
            .toArray(new CompletableFuture[0]));
  }

  @Override
  public SegmentMeta getMeta() {
    if (localSegment != null) {
      return localSegment.getMeta();
    }
    return meta.build();
  }

  @Override
  public CompletableFuture<Void> seal() {
    if ((meta.getFlag() & SEAL_MARK) != 0) {
      return CompletableFuture.completedFuture(null);
    }
    if (localSegment != null) {
      localSegment.seal();
    }
    return dataNodeCnx
        .seal(
            SegmentManagerServiceProto.SealRequest.newBuilder()
                .setTopicId(meta.getTopicId())
                .setPartition(meta.getPartition())
                .setIndex(meta.getIndex())
                .build())
        .thenAccept(
            o -> {
              meta.setFlag(meta.getFlag() & SEAL_MARK);
              meta.setEndOffset(o);
            });
  }

  @Override
  public boolean isSealed() {
    return (meta.getFlag() & SEAL_MARK) != 0;
  }

  @Override
  public void updateMeta(SegmentMeta meta) {
    this.meta = meta.toBuilder();
    this.address = meta.getReplicas(0).getAddress();
  }
}
