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
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.persistence.Partition;
import org.superhx.linky.broker.persistence.PersistenceFactory;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.PartitionServiceGrpc;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.superhx.linky.service.proto.PartitionServiceProto.*;

public class PartitionService extends PartitionServiceGrpc.PartitionServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(PartitionService.class);
  private Map<String, TopicMeta> topicMetas = new ConcurrentHashMap<>();
  private Map<Long, Partition> partitions = new ConcurrentHashMap<>();

  private PersistenceFactory persistenceFactory;

  public CompletableFuture<CloseResponse> close(CloseRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = getTopicPartition(meta.getTopicId(), meta.getPartition());
    Partition partition = partitions.get(topicPartition);
    if (partition == null) {
      return CompletableFuture.completedFuture(
          CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build());
    }
    partition.close();
    partitions.remove(topicPartition);
    return CompletableFuture.completedFuture(
        CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build());
  }

  public CompletableFuture<OpenResponse> open(OpenRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = Utils.topicPartitionId(meta.getTopicId(), meta.getPartition());
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
        .thenApply(r -> OpenResponse.newBuilder().setStatus(OpenResponse.Status.SUCCESS).build());
  }

  public Partition getPartition(int topic, int partition) {
    return partitions.get(Utils.topicPartitionId(topic, partition));
  }

  public Partition getPartition(String topic, int partition) {
    TopicMeta meta = topicMetas.get(topic);
    return getPartition(meta.getId(), partition);
  }

  private static long getTopicPartition(int topic, int partition) {
    return (((long) topic) << 32) & partition;
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

  public void setPersistenceFactory(PersistenceFactory persistenceFactory) {
    this.persistenceFactory = persistenceFactory;
  }
}
