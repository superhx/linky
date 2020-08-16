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
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.SegmentMeta;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface PartitionRegistry extends Lifecycle {
  CompletableFuture<Void> createTopic(String topic, int partitionNum, int replicaNum);

  TopicMeta getTopicMeta(int topicId);

  CompletableFuture<List<PartitionMeta>> getPartitions(int topic);

  CompletableFuture<List<PartitionMeta>> getPartitions(String topic);
}
