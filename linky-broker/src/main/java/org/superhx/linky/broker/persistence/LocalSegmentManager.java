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

import com.google.protobuf.InvalidProtocolBufferException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.loadbalance.SegmentKey;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LocalSegmentManager implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private Map<SegmentKey, Segment> segments = new ConcurrentHashMap<>();
  private DataNodeCnx dataNodeCnx;
  private BrokerContext brokerContext;
  private ChunkManager chunkManager;
  private PersistentMeta persistentMeta;

  @Override
  public void init() {
    try (RocksIterator it = persistentMeta.segmentMetaIterator()) {
      for (it.seekToFirst(); it.isValid(); it.next()) {
        try {
          SegmentMeta meta = SegmentMeta.parseFrom(it.value());
          log.info("[SEGMENT_LOAD]{}", meta);
          segments.put(
              new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex()),
              new LocalSegment(meta, brokerContext, dataNodeCnx, chunkManager));
        } catch (InvalidProtocolBufferException e) {
          log.error("[SEGMENT_META_CORRUPT] unknown meta[{}]", it.value(), e);
          continue;
        }
      }
    }
    for (Segment segment : segments.values()) {
      segment.init();
    }
  }

  @Override
  public void start() {
    for (Segment segment : segments.values()) {
      segment.start();
    }
  }

  @Override
  public void shutdown() {
    for (Segment segment : segments.values()) {
      segment.shutdown();
    }
  }

  public CompletableFuture<Void> createSegment(SegmentMeta meta) {
    SegmentKey key = new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex());
    Segment segment = segments.get(key);
    if (segment != null) {
      log.info("shutdown old local segment {}", meta);
      segment.shutdown();
    }
    persistentMeta.putSegmentMeta(meta.toBuilder().clearReplicas().build());
    segment = new LocalSegment(meta, brokerContext, dataNodeCnx, chunkManager);
    segment.init();
    segment.start();
    segments.put(key, segment);
    log.info("create local segment {}", meta);
    return CompletableFuture.completedFuture(null);
  }

  public CompletableFuture<List<Segment>> getSegments(int topic, int partition) {
    return dataNodeCnx
        .getSegmentMetas(topic, partition)
        .thenCompose(
            metas -> {
              List<Segment> segmentList = new ArrayList<>(metas.size());
              for (SegmentMeta meta : metas) {
                SegmentKey key =
                    new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex());
                Segment segment = segments.get(key);
                segment = new DistributedSegment(meta, segment, dataNodeCnx);
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
              return new DistributedSegment(segment.getMeta(), segment, dataNodeCnx);
            });
  }

  public Segment getSegment(int topic, int partition, int index) {
    return segments.get(new SegmentKey(topic, partition, index));
  }

  public List<SegmentMeta> getLocalSegments() {
    return segments.values().stream().map(s -> s.getMeta()).collect(Collectors.toList());
  }

  public void setDataNodeCnx(DataNodeCnx dataNodeCnx) {
    this.dataNodeCnx = dataNodeCnx;
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  public void setPersistentMeta(PersistentMeta persistentMeta) {
    this.persistentMeta = persistentMeta;
  }
}
