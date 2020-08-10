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
import org.superhx.linky.broker.persistence.LocalSegmentManager;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceGrpc;
import org.superhx.linky.service.proto.ControllerServiceGrpc;
import org.superhx.linky.service.proto.ControllerServiceProto;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class DataNodeCnx {
  private LocalSegmentManager localSegmentManager;
  private BrokerContext brokerContext;
  private Map<String, Channel> channels = new ConcurrentHashMap<>();
  private Channel controllerChannel = getChannel("127.0.0.1:9594");

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

  private SegmentManagerServiceGrpc.SegmentManagerServiceStub getSegmentManagerServiceStub() {
    return SegmentManagerServiceGrpc.newStub(getControllerChannel());
  }

  private ControllerServiceGrpc.ControllerServiceStub getControllerServiceStub() {
    return ControllerServiceGrpc.newStub(getControllerChannel());
  }

  public SegmentServiceGrpc.SegmentServiceStub getSegmentServiceStub(String address) {
    return SegmentServiceGrpc.newStub(getChannel(address));
  }

  private synchronized Channel getChannel(String address) {
    Channel channel = channels.get(address);
    if (channel != null) {
      return channel;
    }
    channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    channels.put(address, channel);
    return channel;
  }

  private Channel getControllerChannel() {
    return this.controllerChannel;
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }
}