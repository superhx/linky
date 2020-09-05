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
package org.superhx.linky.broker.loadbalance;

import com.google.common.base.Strings;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.data.service.proto.SegmentServiceGrpc;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.PartitionServiceGrpc;
import org.superhx.linky.service.proto.PartitionServiceProto;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ControlNodeCnx implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(ControllerService.class);
  private Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

  @Override
  public void shutdown() {
    for (ManagedChannel channel : channels.values()) {
      channel.shutdown();
    }
  }

  public CompletableFuture<Void> closePartition(PartitionMeta partitionMeta) {
    if (partitionMeta == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (Strings.isNullOrEmpty(partitionMeta.getAddress())) {
      CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    getPartitionServiceStub(partitionMeta.getAddress())
        .close(
            PartitionServiceProto.CloseRequest.newBuilder().setMeta(partitionMeta).build(),
            new StreamObserver<PartitionServiceProto.CloseResponse>() {
              @Override
              public void onNext(PartitionServiceProto.CloseResponse response) {
                future.complete(null);
              }

              @Override
              public void onError(Throwable throwable) {
                log.warn("close partition {} fail", partitionMeta, throwable);
                future.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return future;
  }

  public CompletableFuture<PartitionServiceProto.OpenResponse.Status> openPartition(
      PartitionMeta partitionMeta) {
    if (partitionMeta == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (Strings.isNullOrEmpty(partitionMeta.getAddress())) {
      CompletableFuture.completedFuture(null);
    }
    CompletableFuture<PartitionServiceProto.OpenResponse.Status> future = new CompletableFuture<>();
    getPartitionServiceStub(partitionMeta.getAddress())
        .open(
            PartitionServiceProto.OpenRequest.newBuilder().setMeta(partitionMeta).build(),
            new StreamObserver<PartitionServiceProto.OpenResponse>() {
              @Override
              public void onNext(PartitionServiceProto.OpenResponse openResponse) {
                future.complete(openResponse.getStatus());
              }

              @Override
              public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return future;
  }

  public CompletableFuture<Void> createSegment(SegmentMeta segmentMeta) {
    return CompletableFuture.allOf(
        segmentMeta.getReplicasList().stream()
            .map(
                r -> {
                  CompletableFuture<Void> result = new CompletableFuture<>();
                  getSegmentServiceStub(r.getAddress())
                      .create(
                          SegmentServiceProto.CreateRequest.newBuilder()
                              .setSegment(segmentMeta)
                              .build(),
                          new StreamObserver<SegmentServiceProto.CreateResponse>() {
                            @Override
                            public void onNext(SegmentServiceProto.CreateResponse createResponse) {
                              result.complete(null);
                            }

                            @Override
                            public void onError(Throwable throwable) {
                              log.warn(
                                  "request {} create local segment fail {}",
                                  r.getAddress(),
                                  segmentMeta,
                                  throwable);
                              result.completeExceptionally(throwable);
                            }

                            @Override
                            public void onCompleted() {}
                          });
                  return result;
                })
            .collect(Collectors.toList())
            .toArray(new CompletableFuture[0]));
  }

  public CompletableFuture<PartitionServiceProto.StatusResponse> getPartitionStatus(
      String address) {
    CompletableFuture<PartitionServiceProto.StatusResponse> future = new CompletableFuture<>();
    getPartitionServiceStub(address)
        .status(
            PartitionServiceProto.StatusRequest.newBuilder().build(),
            new StreamObserver<PartitionServiceProto.StatusResponse>() {
              @Override
              public void onNext(PartitionServiceProto.StatusResponse statusResponse) {
                future.complete(statusResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                future.completeExceptionally(throwable);
              }

              @Override
              public void onCompleted() {}
            });
    return future;
  }

  public PartitionServiceGrpc.PartitionServiceStub getPartitionServiceStub(String address) {
    Channel channel = getChannel(address);
    return PartitionServiceGrpc.newStub(channel);
  }

  public SegmentServiceGrpc.SegmentServiceStub getSegmentServiceStub(String address) {
    Channel channel = getChannel(address);
    return SegmentServiceGrpc.newStub(channel);
  }

  private Channel getChannel(String address) {
    ManagedChannel channel = channels.get(address);
    if (channel != null) {
      return channel;
    }
    channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    channels.put(address, channel);
    return channel;
  }
}
