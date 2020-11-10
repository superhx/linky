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

import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.loadbalance.SegmentKey;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.service.proto.SegmentMeta;

import java.io.File;
import java.io.IOException;
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

  @Override
  public void init() {
    String segmentsDir = this.brokerContext.getStorePath() + "/segments";
    Utils.ensureDirOK(segmentsDir);
    File dir = new File(segmentsDir);
    File[] topicDirs = dir.listFiles();
    for (File topicDir : topicDirs) {
      int topicId = Integer.valueOf(topicDir.getName());
      File[] partitionDirs = topicDir.listFiles();
      for (File partitionDir : partitionDirs) {
        int partitionId = Integer.valueOf(partitionDir.getName());
        File[] segmentDirs = partitionDir.listFiles();
        for (File segmentDir : segmentDirs) {
          int segmentIndex = Integer.valueOf(segmentDir.getName());
          try {
            String metaPath =
                Utils.getSegmentMetaPath(
                    this.brokerContext.getStorePath(), topicId, partitionId, segmentIndex);
            String metaStr = Utils.file2str(metaPath);
            if (metaStr == null) {
              continue;
            }
            SegmentMeta.Builder builder = SegmentMeta.newBuilder();
            JsonFormat.parser().merge(metaStr, builder);
            SegmentMeta segmentMeta = builder.build();
            log.info("load local segment {}", segmentMeta);
            segments.put(
                new SegmentKey(topicId, partitionId, segmentIndex),
                new LocalSegment(segmentMeta, brokerContext, dataNodeCnx, chunkManager));
          } catch (IOException e) {
            continue;
          }
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
    Utils.byte2file(
        Utils.pb2jsonBytes(meta.toBuilder().clearReplicas()),
        Utils.getSegmentMetaPath(
            this.brokerContext.getStorePath(),
            meta.getTopicId(),
            meta.getPartition(),
            meta.getIndex()));
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

  public CompletableFuture<Segment> nextSegment(
      int topic, int partition, int lastIndex, long startOffset) {
    return dataNodeCnx
        .createSegment(topic, partition, lastIndex, startOffset)
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
}
