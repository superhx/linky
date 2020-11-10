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

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.loadbalance.LinkyElection;
import org.superhx.linky.controller.service.proto.PartitionManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.PartitionManagerServiceProto;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceGrpc;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DataNodeCnx {
  private BrokerContext brokerContext;
  private Map<String, Channel> channels = new ConcurrentHashMap<>();
  private LinkyElection election;

  public DataNodeCnx() {}

  public CompletableFuture<Void> createSegment(
      int topic, int partition, int lastIndex, long startOffset) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    getSegmentManagerServiceStub()
        .create(
            SegmentManagerServiceProto.CreateRequest.newBuilder()
                .setTopicId(topic)
                .setPartition(partition)
                .setLastIndex(lastIndex)
                .setAddress(brokerContext.getAddress())
                .setStartOffset(startOffset)
                .build(),
            new StreamObserver<SegmentManagerServiceProto.CreateResponse>() {
              @Override
              public void onNext(SegmentManagerServiceProto.CreateResponse createResponse) {
                if (createResponse.getStatus()
                    == SegmentManagerServiceProto.CreateResponse.Status.FAIL) {
                  result.completeExceptionally(new LinkyIOException("create segment fail"));
                }
                result.complete(null);
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

  public CompletableFuture<List<SegmentMeta>> getSegmentMetas(int topic, int partition) {
    CompletableFuture<List<SegmentMeta>> result = new CompletableFuture<>();
    getSegmentManagerServiceStub()
        .getSegments(
            SegmentManagerServiceProto.GetSegmentsRequest.newBuilder()
                .setTopicId(topic)
                .setPartition(partition)
                .build(),
            new StreamObserver<SegmentManagerServiceProto.GetSegmentsResponse>() {
              @Override
              public void onNext(
                  SegmentManagerServiceProto.GetSegmentsResponse getSegmentsResponse) {
                result.complete(getSegmentsResponse.getSegmentsList());
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

  public CompletableFuture<Long> seal(SegmentManagerServiceProto.SealRequest request) {
    CompletableFuture<Long> result = new CompletableFuture<>();
    getSegmentManagerServiceStub()
        .seal(
            request,
            new StreamObserver<SegmentManagerServiceProto.SealResponse>() {
              @Override
              public void onNext(SegmentManagerServiceProto.SealResponse sealResponse) {
                if (sealResponse
                    .getStatus()
                    .equals(SegmentManagerServiceProto.SealResponse.Status.FAIL)) {
                  result.completeExceptionally(new LinkyIOException("SEAL_FAIL"));
                  return;
                }
                result.complete(sealResponse.getEndOffset());
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

  public CompletableFuture<Void> keepalive(ControllerServiceProto.HeartbeatRequest request) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    getControllerServiceStub()
        .heartbeat(
            request,
            new StreamObserver<ControllerServiceProto.HeartbeatResponse>() {
              @Override
              public void onNext(ControllerServiceProto.HeartbeatResponse heartbeatResponse) {}

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {}
            });
    return result;
  }

  public void redirectRecordPut(
      PartitionMeta meta, PutRequest putRequest, StreamObserver<PutResponse> responseObserver) {
    getRecordServiceStub(meta.getAddress()).put(putRequest, responseObserver);
  }

  public void redirectRecordGet(
      PartitionMeta meta, GetRequest getRequest, StreamObserver<GetResponse> responseObserver) {
    getRecordServiceStub(meta.getAddress()).get(getRequest, responseObserver);
  }

  public CompletableFuture<Void> reclaim(
      String address, SegmentServiceProto.ReclaimRequest request) {
    CompletableFuture<Void> rst = new CompletableFuture<>();
    getSegmentServiceStub(address)
        .reclaim(
            request,
            new StreamObserver<SegmentServiceProto.ReclaimResponse>() {
              @Override
              public void onNext(SegmentServiceProto.ReclaimResponse reclaimResponse) {
                rst.complete(null);
              }

              @Override
              public void onError(Throwable throwable) {
                rst.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return rst;
  }

  public StreamObserver<PartitionManagerServiceProto.WatchRequest> watchPartition(
      StreamObserver<PartitionManagerServiceProto.WatchResponse> responseObserver) {
    return getPartitionManagerServiceStub().watch(responseObserver);
  }

  private SegmentManagerServiceGrpc.SegmentManagerServiceStub getSegmentManagerServiceStub() {
    return SegmentManagerServiceGrpc.newStub(getControllerChannel());
  }

  private ControllerServiceGrpc.ControllerServiceStub getControllerServiceStub() {
    return ControllerServiceGrpc.newStub(getControllerChannel());
  }

  private PartitionManagerServiceGrpc.PartitionManagerServiceStub getPartitionManagerServiceStub() {
    return PartitionManagerServiceGrpc.newStub(getControllerChannel());
  }

  public SegmentServiceGrpc.SegmentServiceStub getSegmentServiceStub(String address) {
    return SegmentServiceGrpc.newStub(getChannel(address));
  }

  private RecordServiceGrpc.RecordServiceStub getRecordServiceStub(String address) {
    return RecordServiceGrpc.newStub(getChannel(address));
  }

  private synchronized Channel getChannel(String address) {
    if (address == null) {
      return null;
    }
    Channel channel = channels.get(address);
    if (channel != null) {
      return channel;
    }
    channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    channels.put(address, channel);
    return channel;
  }

  private Channel getControllerChannel() {
    return getChannel(election.getLeader().getAddress());
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  public void setElection(LinkyElection election) {
    this.election = election;
  }
}
