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
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.persistence.Partition;
import org.superhx.linky.broker.persistence.PersistenceFactory;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.PartitionServiceGrpc;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.superhx.linky.service.proto.PartitionServiceProto.*;

public class PartitionService extends PartitionServiceGrpc.PartitionServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(PartitionService.class);
  private Map<String, TopicMeta> topicMetas = new ConcurrentHashMap<>();
  private Map<Long, Partition> partitions = new ConcurrentHashMap<>();
  private Map<Long, Semaphore> partitionLocks = new ConcurrentHashMap<>();
  private BrokerContext brokerContext;

  private PersistenceFactory persistenceFactory;

  public CompletableFuture<CloseResponse> close(CloseRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = Utils.topicPartitionId(meta.getTopicId(), meta.getPartition());
    Semaphore lock = getPartitionLock(topicPartition);
    Partition partition = partitions.get(topicPartition);
    if (partition == null || !lock.tryAcquire()) {
      log.info("close {} fail cause of cannot get lock", meta);
      return CompletableFuture.completedFuture(
          CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build());
    }
    return partition
        .close()
        .handle(
            (nil, t) -> {
              partitions.remove(topicPartition);
              lock.release();
              return CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build();
            });
  }

  public CompletableFuture<OpenResponse> open(OpenRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = Utils.topicPartitionId(meta.getTopicId(), meta.getPartition());
    Semaphore lock = getPartitionLock(topicPartition);
    if (!lock.tryAcquire()) {
      log.info("open {} fail cause of cannot get lock", meta);
      return CompletableFuture.completedFuture(
          OpenResponse.newBuilder().setStatus(OpenResponse.Status.FAIL).build());
    }
    Partition partition = partitions.get(topicPartition);
    if (partition == null) {
      partition = persistenceFactory.newPartition(meta);
      partitions.put(topicPartition, partition);
      if (!topicMetas.containsKey(meta.getTopic())) {
        topicMetas.put(meta.getTopic(), TopicMeta.newBuilder().setId(meta.getTopicId()).build());
      }
    }

    return partition
        .open()
        .handle(
            (status, t) -> {
              lock.release();
              if (status == Partition.PartitionStatus.OPEN) {
                return OpenResponse.newBuilder().setStatus(OpenResponse.Status.SUCCESS).build();
              } else {
                if (status != Partition.PartitionStatus.OPENING) {
                  partitions.remove(topicPartition);
                }
                return OpenResponse.newBuilder().setStatus(OpenResponse.Status.FAIL).build();
              }
            });
  }

  public Partition getPartition(int topic, int partition) {
    return partitions.get(Utils.topicPartitionId(topic, partition));
  }

  public Partition getPartition(String topic, int partition) {
    TopicMeta meta = topicMetas.get(topic);
    return getPartition(meta.getId(), partition);
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    close(request)
        .thenAccept(
            (response) -> {
              responseObserver.onNext(response);
              responseObserver.onCompleted();
            });
  }

  @Override
  public void open(OpenRequest request, StreamObserver<OpenResponse> responseObserver) {
    open(request)
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
    List<PartitionMeta> metas =
        partitions.values().stream()
            .filter(
                p ->
                    Partition.PartitionStatus.OPEN.equals(p.status())
                        || Partition.PartitionStatus.OPENING.equals(p.status()))
            .map(p -> p.meta())
            .collect(Collectors.toList());
    responseObserver.onNext(StatusResponse.newBuilder().addAllPartitions(metas).setEpoch(brokerContext.getEpoch()).build());
    responseObserver.onCompleted();
  }

  private synchronized Semaphore getPartitionLock(long topicPartition) {
    if (!partitionLocks.containsKey(topicPartition)) {
      partitionLocks.put(topicPartition, new Semaphore(1));
    }
    return partitionLocks.get(topicPartition);
  }

  public void setPersistenceFactory(PersistenceFactory persistenceFactory) {
    this.persistenceFactory = persistenceFactory;
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }
}
