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
import org.superhx.linky.broker.persistence.Partition;
import org.superhx.linky.service.proto.*;

public class RecordService extends RecordServiceGrpc.RecordServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(RecordService.class);
  private PartitionService partitionService;

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    Partition partition = partitionService.getPartition(request.getTopic(), request.getPartition());
    partition
        .append(request.getBatchRecord())
        .thenAccept(
            appendResult -> {
              PutResponse response =
                  PutResponse.newBuilder()
                      .setStatus(PutResponse.Status.SUCCESS)
                      .setOffset(appendResult.getOffset())
                      .build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              log.warn("record append fail", t);
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    Partition partition = partitionService.getPartition(request.getTopic(), request.getPartition());
    partition
        .get(request.getOffset())
        .thenAccept(
            getResult -> {
              GetResponse response = GetResponse.newBuilder().setBatchRecord(getResult).build();
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              log.warn("get fail", t);
              responseObserver.onError(t);
              return null;
            });
  }

  public void setPartitionService(PartitionService partitionService) {
    this.partitionService = partitionService;
  }
}
