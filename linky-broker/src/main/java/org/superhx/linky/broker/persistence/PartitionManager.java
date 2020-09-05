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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static org.superhx.linky.service.proto.PartitionServiceProto.*;

public class PartitionManager implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(PartitionManager.class);
  /** topic to topic meta map */
  private Map<String, TopicMeta> topicMetas = new ConcurrentHashMap<>();
  /** topic partition id to partition map */
  private Map<Long, Partition> partitions = new ConcurrentHashMap<>();
  /** topic partition id to semaphore map */
  private Map<Long, Semaphore> partitionLocks = new ConcurrentHashMap<>();

  private PersistenceFactory persistenceFactory;

  public CompletableFuture<OpenResponse> open(OpenRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = Utils.topicPartitionId(meta.getTopicId(), meta.getPartition());
    Semaphore lock = getPartitionLock(topicPartition);
    CompletableFuture<OpenResponse> rst = new CompletableFuture<>();
    if (!lock.tryAcquire()) {
      log.info("open {} fail cause of cannot get lock", meta);
      rst.complete(OpenResponse.newBuilder().setStatus(OpenResponse.Status.FAIL).build());
      return rst;
    }

    Partition partition = partitions.get(topicPartition);
    if (partition == null) {
      partition = persistenceFactory.newPartition(meta);
      partitions.put(topicPartition, partition);
      if (!topicMetas.containsKey(meta.getTopic())) {
        topicMetas.put(meta.getTopic(), TopicMeta.newBuilder().setId(meta.getTopicId()).build());
      }
    }

    partition
        .open()
        .handle(
            (status, t) -> {
              lock.release();
              if (status != Partition.PartitionStatus.OPEN) {
                rst.complete(OpenResponse.newBuilder().setStatus(OpenResponse.Status.FAIL).build());
                return null;
              }
              rst.complete(
                  OpenResponse.newBuilder().setStatus(OpenResponse.Status.SUCCESS).build());
              return null;
            });
    return rst;
  }

  public CompletableFuture<CloseResponse> close(CloseRequest request) {
    PartitionMeta meta = request.getMeta();
    long topicPartition = Utils.topicPartitionId(meta.getTopicId(), meta.getPartition());
    Semaphore lock = getPartitionLock(topicPartition);
    CompletableFuture<CloseResponse> rst = new CompletableFuture<>();
    Partition partition = partitions.get(topicPartition);
    if (partition == null || !lock.tryAcquire()) {
      log.info("close {} fail cause of cannot get lock", meta);
      rst.complete(CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build());
      return rst;
    }
    partition
        .close()
        .handle(
            (nil, t) -> {
              partitions.remove(topicPartition);
              lock.release();
              rst.complete(
                  CloseResponse.newBuilder().setStatus(CloseResponse.Status.SUCCESS).build());
              return null;
            });
    return rst;
  }

  public Partition getPartition(int topic, int partition) {
    return partitions.get(Utils.topicPartitionId(topic, partition));
  }

  public Partition getPartition(String topic, int partition) {
    TopicMeta meta = topicMetas.get(topic);
    return getPartition(meta.getId(), partition);
  }

  public List<PartitionMeta> getPartitions() {
    List<PartitionMeta> metas =
        partitions.values().stream()
            .filter(
                p ->
                    Partition.PartitionStatus.OPEN.equals(p.status())
                        || Partition.PartitionStatus.OPENING.equals(p.status()))
            .map(p -> p.meta())
            .collect(Collectors.toList());
    return metas;
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
}
