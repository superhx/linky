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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.persistence.Partition;
import org.superhx.linky.broker.persistence.PartitionManager;
import org.superhx.linky.controller.service.proto.PartitionManagerServiceProto;
import org.superhx.linky.service.proto.*;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class RecordService extends RecordServiceGrpc.RecordServiceImplBase implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(RecordService.class);
  private PartitionManager partitionManager;
  private DataNodeCnx dataNodeCnx;
  private Map<String, Map<Integer, PartitionMeta>> partitions = new ConcurrentHashMap<>();

  @Override
  public void start() {
    dataNodeCnx.watchPartition(new PartitionWatchHandler());
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    Partition partition = partitionManager.getPartition(request.getTopic(), request.getPartition());
    if (partition == null) {
      PartitionMeta meta =
          Optional.ofNullable(partitions.get(request.getTopic()))
              .map(m -> m.get(request.getPartition()))
              .get();
      dataNodeCnx.redirectRecordPut(meta, request, responseObserver);
      return;
    }
    partition
        .append(request.getBatchRecord())
        .thenAccept(
            appendResult -> {
              PutResponse response =
                  PutResponse.newBuilder()
                      .setStatus(PutResponse.Status.SUCCESS)
                      .setCursor(ByteString.copyFrom(appendResult.getCursor()))
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
    Partition partition = partitionManager.getPartition(request.getTopic(), request.getPartition());
    if (partition == null) {
      PartitionMeta meta =
          Optional.ofNullable(partitions.get(request.getTopic()))
              .map(m -> m.get(request.getPartition()))
              .get();
      dataNodeCnx.redirectRecordGet(meta, request, responseObserver);
      return;
    }
    partition
        .get(request.getCursor().toByteArray())
        .thenAccept(
            getResult -> {
              GetResponse.Builder response =
                  GetResponse.newBuilder()
                      .setNextCursor(ByteString.copyFrom(getResult.getNextCursor()));
              switch (getResult.getStatus()) {
                case SUCCESS:
                  response.setBatchRecord(getResult.getBatchRecord());
                  break;
                case NO_NEW_MSG:
                  response.setStatus(GetResponse.Status.NO_NEW_MSG);
              }
              responseObserver.onNext(response.build());
              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              log.warn("get fail", t);
              responseObserver.onError(t);
              return null;
            });
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }

  public void setDataNodeCnx(DataNodeCnx dataNodeCnx) {
    this.dataNodeCnx = dataNodeCnx;
  }

  class PartitionWatchHandler
      implements StreamObserver<PartitionManagerServiceProto.WatchResponse> {

    @Override
    public void onNext(PartitionManagerServiceProto.WatchResponse watchResponse) {
      for (PartitionMeta partition : watchResponse.getPartitionsList()) {
        if (!partitions.containsKey(partition.getTopic())) {
          partitions.put(partition.getTopic(), new ConcurrentHashMap<>());
        }
        PartitionMeta old =
            partitions.get(partition.getTopic()).put(partition.getPartition(), partition);
        if (old == null || !Objects.equals(old.getAddress(), partition.getAddress()))
          log.info("[PARTITION_CHANGE] old: {} new: {}", old, partition);
      }
    }

    @Override
    public void onError(Throwable throwable) {
      dataNodeCnx.watchPartition(new PartitionWatchHandler());
    }

    @Override
    public void onCompleted() {
      dataNodeCnx.watchPartition(new PartitionWatchHandler());
    }
  }
}
