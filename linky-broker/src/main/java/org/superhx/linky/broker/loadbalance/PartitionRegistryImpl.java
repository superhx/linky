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

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.PartitionServiceProto;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartitionRegistryImpl
    implements PartitionRegistry, Lifecycle, LinkyElection.LeaderChangeListener {
  private static final Logger log = LoggerFactory.getLogger(PartitionRegistryImpl.class);
  private AtomicInteger topicIdCounter = new AtomicInteger();
  private Map<Integer, TopicMeta> topicIdMetaMap = new ConcurrentHashMap<>();
  private Map<String, TopicMeta> topicMetas = new ConcurrentHashMap<>();
  private Map<Integer, Map<Integer, PartitionMeta>> topicPartitionMap = new ConcurrentHashMap<>();
  private ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();

  private NodeRegistry nodeRegistry;
  private ControlNodeCnx controlNodeCnx;
  private LinkyElection election;
  private KVStore kvStore;
  private BrokerContext brokerContext;

  @Override
  public void start() {
    schedule.scheduleWithFixedDelay(() -> rebalance(), 1000, 5000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    schedule.shutdown();
  }

  @Override
  public CompletableFuture<Void> createTopic(String topic, int partitionNum, int replicaNum) {
    if (topicMetas.containsKey(topic)) {
      return CompletableFuture.completedFuture(null);
    }
    int id = topicIdCounter.incrementAndGet();
    TopicMeta meta =
        TopicMeta.newBuilder()
            .setTopic(topic)
            .setId(id)
            .setPartitionNum(partitionNum)
            .setReplicaNum(replicaNum)
            .build();
    topicPartitionMap.put(id, new ConcurrentHashMap<>());
    topicMetas.put(topic, meta);
    topicIdMetaMap.put(id, meta);
    kvStore.put("topics/" + id, meta.toByteArray());
    return rebalance().thenAccept(r -> {});
  }

  @Override
  public TopicMeta getTopicMeta(int topicId) {
    return topicIdMetaMap.get(topicId);
  }

  @Override
  public CompletableFuture<List<PartitionMeta>> getPartitions(int topic) {
    TopicMeta meta = topicIdMetaMap.get(topic);
    List<PartitionMeta> partitions = new ArrayList<>(topicPartitionMap.get(topic).values());
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

  private CompletableFuture<Void> rebalance() {
    try {
      rebalance0();
    } catch (Throwable e) {
      log.error("rebalance fail", e);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void rebalance0() {
    if (!election.isLeader()) {
      return;
    }
    new Rebalance().run();
  }

  public void setNodeRegistry(NodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

  public void setControlNodeCnx(ControlNodeCnx controlNodeCnx) {
    this.controlNodeCnx = controlNodeCnx;
  }

  public void setKvStore(KVStore kvStore) {
    this.kvStore = kvStore;
  }

  public void setElection(LinkyElection election) {
    this.election = election;
  }

  @Override
  public void onChanged(LinkyElection.Leader leader) {
    try {
      onChanged0(leader);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void onChanged0(LinkyElection.Leader leader)
      throws ExecutionException, InterruptedException {
    if (!leader.isCurrentNode()) {
      return;
    }
    kvStore
        .get("topics/")
        .thenAccept(
            r ->
                r.getKvs().stream()
                    .forEach(
                        m -> {
                          try {
                            TopicMeta meta = TopicMeta.parseFrom(m.getValue().getBytes());
                            topicIdMetaMap.put(meta.getId(), meta);
                            topicMetas.put(meta.getTopic(), meta);
                            topicPartitionMap.put(meta.getId(), new ConcurrentHashMap<>());
                            if (topicIdCounter.get() < meta.getId()) {
                              topicIdCounter.set(meta.getId());
                            }
                            log.info("load topic config {}", meta);
                          } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                          }
                        }))
        .get();
    List<String> nodes =
        kvStore
            .get("controller/node/")
            .thenApply(
                rst ->
                    rst.getKvs().stream()
                        .map(kv -> kv.getValue().toString(Utils.DEFAULT_CHARSET))
                        .collect(Collectors.toList()))
            .get();

    CompletableFuture.allOf(
            nodes.stream()
                .map(
                    node ->
                        controlNodeCnx
                            .getPartitionStatus(node)
                            .thenAccept(
                                resp -> {
                                  nodeRegistry.register(
                                      NodeMeta.newBuilder()
                                          .setAddress(node)
                                          .setEpoch(resp.getEpoch())
                                          .build());
                                  log.info(
                                      "get {} partition status {}", node, resp.getPartitionsList());
                                  resp.getPartitionsList()
                                      .forEach(
                                          meta -> {
                                            if (!topicPartitionMap.containsKey(meta.getTopicId())) {
                                              topicPartitionMap.put(
                                                  meta.getTopicId(), new ConcurrentHashMap<>());
                                            }
                                            topicPartitionMap
                                                .get(meta.getTopicId())
                                                .put(meta.getPartition(), meta);
                                          });
                                })
                            .exceptionally(
                                t -> {
                                  log.warn("get {} partition status fail", node, t);
                                  return null;
                                }))
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .get();
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  class Rebalance implements Runnable {
    private Map<Integer, List<PartitionMeta>> newTopicPartitionMap;
    private List<NodeMeta> nodes;
    private Set<NodeMeta> nodeSet;
    private Map<NodeMeta, AtomicInteger> nodePartitionCount;

    public Rebalance() {
      newTopicPartitionMap = new HashMap<>();
      nodes = new ArrayList<>(nodeRegistry.getAliveNodes());
      nodeSet = new HashSet<>(nodes);
      nodePartitionCount = new HashMap<>();
      for (NodeMeta nodeMeta : nodes) {
        nodePartitionCount.put(nodeMeta, new AtomicInteger());
      }
    }

    @Override
    public void run() {
      rebalance0();
    }

    private void rebalance0() {
      if (nodes.size() == 0) {
        log.info("cannot find node in cluster, exit rebalance");
        return;
      }
      fastAllocate();
      closeOpen();
    }

    protected void fastAllocate() {
      int partitionCount = 0;
      for (TopicMeta topicMeta : topicMetas.values()) {
        List<PartitionMeta> partitionMetas = new ArrayList<>(topicMeta.getPartitionNum());
        for (int i = 0; i < topicMeta.getPartitionNum(); i++) {
          int partition = i;
          PartitionMeta partitionMeta =
              Optional.ofNullable(topicPartitionMap.get(topicMeta.getId()))
                  .map(m -> m.get(partition))
                  .orElse(null);
          if (partitionMeta == null
              || !nodeSet.contains(Utils.partitionMeta2NodeMeta(partitionMeta))) {
            partitionMeta =
                chooseNode(
                        PartitionMeta.newBuilder()
                            .setTopicId(topicMeta.getId())
                            .setTopic(topicIdMetaMap.get(topicMeta.getId()).getTopic())
                            .setPartition(i),
                        partitionCount)
                    .build();
          }
          nodePartitionCount.get(Utils.partitionMeta2NodeMeta(partitionMeta)).incrementAndGet();
          partitionCount++;
          if (topicPartitionMap.containsKey(topicMeta.getId())
              && topicPartitionMap.get(topicMeta.getId()).containsKey(i)) {}

          partitionMetas.add(partitionMeta);
        }
        newTopicPartitionMap.put(topicMeta.getId(), partitionMetas);
      }
    }

    protected PartitionMeta.Builder chooseNode(PartitionMeta.Builder meta, int partitionCount) {
      NodeMeta nodeMeta = null;
      if (nodes.size() == 1) {
        nodeMeta = nodes.get(0);
      }
      if (nodeMeta == null) {
        for (int i = partitionCount; i < partitionCount + nodes.size(); i++) {
          NodeMeta node = nodes.get(i % nodes.size());
          if (Objects.equals(node.getAddress(), brokerContext.getAddress())) {
            continue;
          }
          if (((double) partitionCount + 1) / (nodes.size() - 1)
              > nodePartitionCount.get(node).get()) {
            nodeMeta = node;
            break;
          }
        }
      }
      if (nodeMeta == null) {
        nodeMeta = nodes.get(partitionCount % nodes.size());
      }
      meta.setAddress(nodeMeta.getAddress()).setEpoch(nodeMeta.getEpoch());
      return meta;
    }

    protected void closeOpen() {
      List<CompletableFuture<?>> futures = new LinkedList<>();
      for (Integer topic : newTopicPartitionMap.keySet()) {
        List<PartitionMeta> newPartitionMetas = newTopicPartitionMap.get(topic);
        List<PartitionMeta> oldPartitionMetas =
            new ArrayList<>(topicPartitionMap.get(topic).values());
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
          if (nodeSet.contains(Utils.partitionMeta2NodeMeta(oldPartitionMeta))) {
            close.add(oldPartitionMeta);
          } else {
            log.info("skip close partition {} cause of node down", oldPartitionMeta);
            close.add(null);
          }
          open.add(newPartitionMeta);
        }
        for (; partitionIndex < newPartitionMetas.size(); partitionIndex++) {
          close.add(null);
          open.add(newPartitionMetas.get(partitionIndex));
        }
        for (; partitionIndex < oldPartitionMetas.size(); partitionIndex++) {
          close.add(oldPartitionMetas.get(partitionIndex));
          open.add(null);
        }
        if (open.size() == 0 && close.size() == 0) {
          continue;
        }
        for (int i = 0; i < open.size(); i++) {
          PartitionMeta closeMeta = close.get(i);
          PartitionMeta openMeta = open.get(i);
          futures.add(
              controlNodeCnx
                  .closePartition(closeMeta)
                  .handle(
                      (r, t) -> {
                        if (t != null) {
                          log.warn("close {} fail", closeMeta, t);
                        }
                        if (closeMeta != null) {
                          topicPartitionMap.get(topic).remove(closeMeta.getPartition());
                        }
                        return controlNodeCnx.openPartition(openMeta);
                      })
                  .thenCompose(f -> f)
                  .thenAccept(
                      r -> {
                        if (r == PartitionServiceProto.OpenResponse.Status.SUCCESS) {
                          topicPartitionMap.get(topic).put(openMeta.getPartition(), openMeta);
                        }
                      })
                  .exceptionally(
                      t -> {
                        topicPartitionMap.get(topic).remove(openMeta.getPartition());
                        return null;
                      }));
        }
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
  }
}
