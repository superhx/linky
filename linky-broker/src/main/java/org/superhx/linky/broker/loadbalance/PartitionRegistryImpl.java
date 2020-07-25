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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.SegmentMeta;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartitionRegistryImpl implements PartitionRegistry {
  private static final Logger log = LoggerFactory.getLogger(PartitionRegistryImpl.class);
  private AtomicInteger topicIdCounter = new AtomicInteger();
  private Map<Integer, TopicMeta> topicIdMetaMap = new ConcurrentHashMap<>();
  private Map<String, TopicMeta> topicMetas = new ConcurrentHashMap<>();
  private Map<Integer, List<PartitionMeta>> topicPartitionMap = new ConcurrentHashMap<>();
  private Map<Long, List<Integer>> partitionSegmentMap = new ConcurrentHashMap<>();
  private ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();

  private NodeRegistry nodeRegistry;
  private SegmentRegistry segmentRegistry;
  private ControlNodeCnx controlNodeCnx;

  public void start() {
    schedule.scheduleWithFixedDelay(() -> rebalance(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<Void> createTopic(String topic, int partitionNum, int replicaNum) {
    int id = topicIdCounter.incrementAndGet();
    TopicMeta meta =
        TopicMeta.newBuilder()
            .setTopic(topic)
            .setId(id)
            .setPartitionNum(partitionNum)
            .setReplicaNum(replicaNum)
            .build();
    topicMetas.put(topic, meta);
    topicIdMetaMap.put(id, meta);
    return rebalance().thenAccept(r -> {});
  }

  @Override
  public CompletableFuture<List<PartitionMeta>> getPartitions(int topic) {
    TopicMeta meta = topicIdMetaMap.get(topic);
    List<PartitionMeta> partitions = topicPartitionMap.get(topic);
    if (partitions == null) {
      // TODO: fill blank partitions
    }
    return CompletableFuture.completedFuture(partitions);
  }

  @Override
  public CompletableFuture<List<PartitionMeta>> getPartitions(String topic) {
    TopicMeta meta = topicMetas.get(topic);
    return getPartitions(meta.getId());
  }

  @Override
  public CompletableFuture<List<SegmentMeta>> getSegments(int topic, int partition) {
    long topicPartitionId = Utils.topicPartitionId(topic, partition);
    List<Integer> segmentIndexes = partitionSegmentMap.get(topicPartitionId);
    if (segmentIndexes == null || segmentIndexes.size() == 0) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    Map<Integer, SegmentMeta> indexSegmentMap = segmentRegistry.getSegmentMetas(topic, partition);
    for (Integer index : segmentIndexes) {
      if (!indexSegmentMap.containsKey(index)) {
        // TODO; none segment
        throw new RuntimeException("replica lost");
      }
    }
    return CompletableFuture.completedFuture(
        indexSegmentMap.values().stream()
            .sorted(Comparator.comparingInt(SegmentMeta::getIndex))
            .collect(Collectors.toList()));
  }

  private CompletableFuture<Void> rebalance() {
    rebalance0();
    return CompletableFuture.completedFuture(null);
  }

  private void rebalance0() {
    Map<Integer, List<PartitionMeta>> newTopicPartitionMap = new HashMap<>();
    List<TopicMeta> topicMetas = new ArrayList<>(topicIdMetaMap.values());
    Collections.sort(topicMetas, Comparator.comparing(TopicMeta::getTopic));
    List<NodeMeta> nodes = new ArrayList<>(nodeRegistry.getAliveNodes());
    Collections.sort(nodes, Comparator.comparing(NodeMeta::getAddress));
    if (nodes.size() == 0) {
      log.info("cannot find node in cluster, exit rebalance");
      return;
    }

    int counter = 0;
    for (TopicMeta topicMeta : topicMetas) {
      List<PartitionMeta> partitionMetas = new ArrayList<>(topicMeta.getPartitionNum());
      for (int i = 0; i < topicMeta.getPartitionNum(); i++) {
        partitionMetas.add(
            PartitionMeta.newBuilder()
                .setTopicId(topicMeta.getId())
                .setTopic(topicIdMetaMap.get(topicMeta.getId()).getTopic())
                .setPartition(i)
                .setAddress(nodes.get(counter % nodes.size()).getAddress())
                .build());
        counter++;
      }
      newTopicPartitionMap.put(topicMeta.getId(), partitionMetas);
    }

    for (Integer topic : newTopicPartitionMap.keySet()) {
      List<PartitionMeta> newPartitionMetas = newTopicPartitionMap.get(topic);
      List<PartitionMeta> oldPartitionMetas =
          topicPartitionMap.getOrDefault(topic, Collections.emptyList());
      List<PartitionMeta> open = new LinkedList<>();
      List<PartitionMeta> close = new LinkedList<>();
      int partitionIndex = 0;
      for (;
          partitionIndex < newPartitionMetas.size() && partitionIndex < oldPartitionMetas.size();
          partitionIndex++) {
        PartitionMeta newPartitionMeta = newPartitionMetas.get(partitionIndex);
        PartitionMeta oldPartitionMeta = oldPartitionMetas.get(partitionIndex);
        if (newPartitionMeta.equals(oldPartitionMeta)) {
          continue;
        }
        close.add(oldPartitionMeta);
        open.add(newPartitionMeta);
      }
      for (; partitionIndex < newPartitionMetas.size(); partitionIndex++) {
        open.add(newPartitionMetas.get(partitionIndex));
      }
      for (; partitionIndex < oldPartitionMetas.size(); partitionIndex++) {
        close.add(oldPartitionMetas.get(partitionIndex));
      }
      if (open.size() == 0 && close.size() == 0) {
        continue;
      }
      CompletableFuture.allOf(
              close.stream()
                  .map(meta -> controlNodeCnx.closePartition(meta))
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .join();
      CompletableFuture.allOf(
              open.stream()
                  .map(meta -> controlNodeCnx.openPartition(meta).thenAccept(r -> {}))
                  .collect(Collectors.toList())
                  .toArray(new CompletableFuture[0]))
          .join();
      topicPartitionMap.put(topic, newPartitionMetas);
    }
  }

  public void setNodeRegistry(NodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

  public void setSegmentRegistry(SegmentRegistry segmentRegistry) {
    this.segmentRegistry = segmentRegistry;
  }

  public void setControlNodeCnx(ControlNodeCnx controlNodeCnx) {
    this.controlNodeCnx = controlNodeCnx;
  }
}
