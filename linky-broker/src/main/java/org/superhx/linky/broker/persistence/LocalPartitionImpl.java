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
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.PartitionMeta;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalPartitionImpl implements Partition {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private PartitionMeta meta;

  private List<Segment> segments;
  private volatile Segment lastSegment;
  private LocalSegmentManager localSegmentManager;

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    Segment segment = this.lastSegment;
    return segment
        .append(batchRecord)
        .thenApply(appendResult -> new AppendResult(appendResult.getOffset()));
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    Segment segment = this.lastSegment;
    return segment.get(offset);
  }

  @Override
  public CompletableFuture<Void> open() {
    log.info("partition {} opening...", meta);
    return localSegmentManager
        .getSegments(meta.getTopicId(), meta.getPartition())
        .thenCompose(
            s -> {
              segments = new CopyOnWriteArrayList<>(s);
              return localSegmentManager.nextSegment(
                  meta.getTopicId(),
                  meta.getPartition(),
                  segments.isEmpty()
                      ? Segment.NO_INDEX
                      : segments.get(segments.size() - 1).getIndex());
            })
        .thenCompose(
            s -> {
              this.lastSegment = s;
              log.info("partition {} opened", meta);
              return CompletableFuture.completedFuture(null);
            });
  }

  @Override
  public CompletableFuture<Void> close() {
    segments = null;
    if (lastSegment == null) {
      return CompletableFuture.completedFuture(null);
    }
    return lastSegment.seal().thenCompose(s -> CompletableFuture.completedFuture(null));
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }
}
