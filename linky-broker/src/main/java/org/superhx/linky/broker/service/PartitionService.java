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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.persistence.PartitionManager;
import org.superhx.linky.service.proto.PartitionServiceGrpc;

import static org.superhx.linky.service.proto.PartitionServiceProto.*;

public class PartitionService extends PartitionServiceGrpc.PartitionServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(PartitionService.class);
  private PartitionManager partitionManager;
  private BrokerContext brokerContext;

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    partitionManager
        .close(request)
        .thenAccept(
            resp -> {
              responseObserver.onNext(resp);
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              log.warn("partition close {} fail", request.getMeta(), t);
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void open(OpenRequest request, StreamObserver<OpenResponse> responseObserver) {
    partitionManager
        .open(request)
        .thenAccept(
            (response) -> {
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              log.warn("partition open {} fail", request.getMeta(), t);
              responseObserver.onError(t);
              responseObserver.onCompleted();
              return null;
            });
  }

  @Override
  public void status(StatusRequest request, StreamObserver<StatusResponse> responseObserver) {
    responseObserver.onNext(
        StatusResponse.newBuilder()
            .addAllPartitions(partitionManager.getPartitions())
            .setEpoch(brokerContext.getEpoch())
            .build());
    responseObserver.onCompleted();
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
