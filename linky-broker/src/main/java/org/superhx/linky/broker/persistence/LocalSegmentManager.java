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
import org.superhx.linky.broker.loadbalance.SegmentKey;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class LocalSegmentManager {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private Map<SegmentKey, Segment> segments = new ConcurrentHashMap<>();

  private PersistenceFactory persistenceFactory;
  private DataNodeCnx dataNodeCnx;

  public LocalSegmentManager() {}

  public CompletableFuture<Void> createSegment(SegmentMeta meta) {
    Segment segment = persistenceFactory.newSegment(meta);
    segments.put(new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex()), segment);
    log.info("create local segment {}", meta);
    return CompletableFuture.completedFuture(null);
  }

  public CompletableFuture<List<Segment>> getSegments(int topic, int partition) {
    return dataNodeCnx
        .getSegmentMetas(topic, partition)
        .exceptionally(
            t -> {
              t.printStackTrace();
              return null;
            })
        .thenCompose(
            metas -> {
              List<Segment> segmentList = new ArrayList<>(metas.size());
              for (SegmentMeta meta : metas) {
                SegmentKey key =
                    new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex());
                Segment segment = segments.get(key);
                if (segment == null) {
                  segment = persistenceFactory.newSegment(meta);
                }
                segmentList.add(segment);
              }
              return CompletableFuture.completedFuture(segmentList);
            });
  }

  public CompletableFuture<Segment> nextSegment(int topic, int partition, int lastIndex) {
    return dataNodeCnx
        .createSegment(topic, partition, lastIndex)
        .thenApply(
            r -> {
              SegmentKey key = new SegmentKey(topic, partition, lastIndex + 1);
              Segment segment = segments.get(key);
              if (segment == null) {
                throw new IllegalStateException(String.format("cannot find segment %s", key));
              }
              log.info("create segment {}", segment);
              return segment;
            });
  }

  public Segment getSegment(int topic, int partition, int index) {
    return segments.get(new SegmentKey(topic, partition, index));
  }

  public void setPersistenceFactory(PersistenceFactory persistenceFactory) {
    this.persistenceFactory = persistenceFactory;
  }

  public void setDataNodeCnx(DataNodeCnx dataNodeCnx) {
    this.dataNodeCnx = dataNodeCnx;
  }

  public static void main(String... args) {
    CompletableFuture<String> result = new CompletableFuture<>();
    result
        .thenApply(
            s -> {
              System.out.println("hello" + s);
              return null;
            })
        .exceptionally(
            t -> {
              t.printStackTrace();
              return null;
            });
    result.completeExceptionally(new RuntimeException());
  }
}
