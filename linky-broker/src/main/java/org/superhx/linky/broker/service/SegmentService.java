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
package org.superhx.linky.broker.service;

import io.grpc.stub.StreamObserver;
import org.superhx.linky.broker.persistence.LocalSegmentManager;
import org.superhx.linky.broker.persistence.Segment;
import org.superhx.linky.data.service.proto.SegmentServiceGrpc;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.SegmentMeta;

public class SegmentService extends SegmentServiceGrpc.SegmentServiceImplBase {

  private LocalSegmentManager localSegmentManager;

  public SegmentService() {}

  @Override
  public void get(
      SegmentServiceProto.GetRecordRequest request,
      StreamObserver<SegmentServiceProto.GetRecordResponse> responseObserver) {
    Segment segment =
        localSegmentManager.getSegment(
            request.getTopicId(), request.getPartition(), request.getIndex());
    segment
        .get(request.getOffset())
        .thenAccept(
            r -> {
              responseObserver.onNext(
                  SegmentServiceProto.GetRecordResponse.newBuilder().setBatchRecord(r).build());
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public StreamObserver<SegmentServiceProto.ReplicateRequest> replicate(
      StreamObserver<SegmentServiceProto.ReplicateResponse> responseObserver) {
    return new StreamObserver<SegmentServiceProto.ReplicateRequest>() {
      @Override
      public void onNext(SegmentServiceProto.ReplicateRequest replicateRequest) {
        Segment segment =
            localSegmentManager.getSegment(
                replicateRequest.getBatchRecord().getTopicId(),
                replicateRequest.getBatchRecord().getPartition(),
                replicateRequest.getBatchRecord().getIndex());
        if (segment == null) {
          responseObserver.onNext(
              SegmentServiceProto.ReplicateResponse.newBuilder()
                  .setStatus(SegmentServiceProto.ReplicateResponse.Status.NOT_FOUND)
                  .build());
          responseObserver.onCompleted();
          return;
        }
        segment.replicate(replicateRequest, responseObserver);
      }

      @Override
      public void onError(Throwable throwable) {
        throwable.printStackTrace();
      }

      @Override
      public void onCompleted() {}
    };
  }

  @Override
  public void create(
      SegmentServiceProto.CreateRequest request,
      StreamObserver<SegmentServiceProto.CreateResponse> responseObserver) {
    localSegmentManager
        .createSegment(request.getSegment())
        .thenAccept(
            r -> {
              responseObserver.onNext(SegmentServiceProto.CreateResponse.newBuilder().build());
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void seal(
      SegmentServiceProto.SealRequest request,
      StreamObserver<SegmentServiceProto.SealResponse> responseObserver) {
    Segment segment =
        this.localSegmentManager.getSegment(
            request.getTopicId(), request.getPartition(), request.getIndex());
    segment
        .seal()
        .thenAccept(
            r -> {
              responseObserver.onNext(
                  SegmentServiceProto.SealResponse.newBuilder()
                      .setEndOffset(segment.getEndOffset())
                      .build());
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void reclaim(
      SegmentServiceProto.ReclaimRequest request,
      StreamObserver<SegmentServiceProto.ReclaimResponse> responseObserver) {
    Segment segment =
        this.localSegmentManager.getSegment(
            request.getTopicId(), request.getPartition(), request.getIndex());
    segment
        .reclaimSpace(request.getOffset())
        .thenAccept(
            nil -> {
              responseObserver.onNext(SegmentServiceProto.ReclaimResponse.newBuilder().build());
              responseObserver.onCompleted();
            });
  }

  @Override
  public void status(
      SegmentServiceProto.StatusRequest request,
      StreamObserver<SegmentServiceProto.StatusResponse> responseObserver) {
    Segment segment =
        localSegmentManager.getSegment(
            request.getTopicId(), request.getPartition(), request.getIndex());
    responseObserver.onNext(
        SegmentServiceProto.StatusResponse.newBuilder().setMeta(segment.getMeta()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void update(
      SegmentServiceProto.UpdateRequest request,
      StreamObserver<SegmentServiceProto.UpdateResponse> responseObserver) {
    SegmentMeta meta = request.getMeta();
    Segment segment =
        this.localSegmentManager.getSegment(
            meta.getTopicId(), meta.getPartition(), meta.getIndex());
    segment.updateMeta(meta);
    responseObserver.onNext(SegmentServiceProto.UpdateResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void getTimerSlot(
      SegmentServiceProto.GetTimerSlotRequest request,
      StreamObserver<SegmentServiceProto.GetTimerSlotResponse> responseObserver) {
    Segment segment =
        this.localSegmentManager.getSegment(
            request.getTopicId(), request.getPartition(), request.getIndex());
    try {
      responseObserver.onNext(
          SegmentServiceProto.GetTimerSlotResponse.newBuilder()
              .addAllTimerIndexes(segment.getTimerSlot(request.getOffset()).get())
              .build());
    } catch (Exception e) {
      e.printStackTrace();
      responseObserver.onError(e);
    }
    responseObserver.onCompleted();
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }
}
