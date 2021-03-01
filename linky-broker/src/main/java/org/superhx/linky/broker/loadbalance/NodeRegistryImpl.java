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

import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.service.proto.NodeMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class NodeRegistryImpl implements NodeRegistry, Lifecycle {
  private Map<String, Timestamped<NodeMeta>> nodeMap = new ConcurrentHashMap<>();
  private static final long TIMEOUT = 5_000;

  @Override
  public CompletableFuture<Void> register(NodeMeta node) {
    nodeMap.put(node.getAddress(), new Timestamped<>(node));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public List<NodeMeta> getOnlineNodes() {
    List<NodeMeta> nodes = new ArrayList<>(nodeMap.size());
    nodes.addAll(
        nodeMap.values().stream()
            .filter(t -> (System.currentTimeMillis() - t.getTimestamp()) < TIMEOUT)
            .map(n -> n.getData())
            .filter(n -> n.getStatus() == NodeMeta.Status.ONLINE)
            .collect(Collectors.toList()));
    return nodes;
  }

  @Override
  public List<NodeMeta> getAliveNodes() {
    List<NodeMeta> nodes = new ArrayList<>(nodeMap.size());
    nodes.addAll(
        nodeMap.values().stream()
            .filter(t -> (System.currentTimeMillis() - t.getTimestamp()) < TIMEOUT)
            .map(n -> n.getData())
            .collect(Collectors.toList()));
    return nodes;
  }
}
