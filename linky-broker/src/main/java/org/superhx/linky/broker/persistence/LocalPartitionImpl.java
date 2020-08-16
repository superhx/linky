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
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.PartitionMeta;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/** index format [physical offset 8bytes][size 4bytes] 不同的索引可以索引到同一消息，然后通过消息里面的offset做过滤 */
public class LocalPartitionImpl implements Partition {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private PartitionMeta meta;
  private AtomicReference<PartitionStatus> status = new AtomicReference<>(PartitionStatus.NOOP);
  private List<Segment> segments;
  private volatile Segment lastSegment;
  private NavigableMap<Long, Segment> segmentStartOffsets = new ConcurrentSkipListMap<>();
  private LocalSegmentManager localSegmentManager;
  private CompletableFuture<Segment> lastSegmentFuture;
  private Map<Segment, CompletableFuture<Segment>> nextSegmentFutures = new ConcurrentHashMap<>();

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
  }

  class FlushTask implements Runnable {
    @Override
    public void run() {}
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    return getLastSegment()
        .thenCompose(
            s -> {
              switch (s.getStatus()) {
                case WRITABLE:
                  return s.append(batchRecord);
                case REPLICA_LOSS:
                case REPLICA_BREAK:
                  return nextSegment(s).thenCompose(n -> n.append(batchRecord));
              }
              throw new LinkyIOException(String.format("Unknown status %s", s.getStatus()));
            })
        .thenApply(appendResult -> new AppendResult(appendResult.getOffset()));
  }

  protected CompletableFuture<Segment> getLastSegment() {
    if (lastSegmentFuture != null) {
      return lastSegmentFuture;
    }
    synchronized (this) {
      if (lastSegmentFuture != null) {
        return lastSegmentFuture;
      }

      long nextSegmentStartOffset = 0;
      for (int i = 0; i < segments.size(); i++) {
        Segment segment = segments.get(i);
        if (segment.getStartOffset() < segment.getEndOffset()) {
          nextSegmentStartOffset = segments.get(i).getEndOffset();
        }
      }
      lastSegmentFuture = new CompletableFuture();
      localSegmentManager
          .nextSegment(
              meta.getTopicId(),
              meta.getPartition(),
              segments.isEmpty() ? Segment.NO_INDEX : segments.get(segments.size() - 1).getIndex(),
              nextSegmentStartOffset)
          .thenAccept(
              s -> {
                lastSegmentFuture.complete(s);
                this.lastSegment = s;
                segmentStartOffsets.put(this.lastSegment.getStartOffset(), this.lastSegment);
                segments.add(this.lastSegment);
              });
      return lastSegmentFuture;
    }
  }

  protected CompletableFuture<Segment> nextSegment(Segment lastSegment) {
    CompletableFuture<Segment> future = nextSegmentFutures.get(lastSegment);
    if (future != null) {
      return future;
    }
    synchronized (this) {
      future = nextSegmentFutures.get(lastSegment);
      if (future != null) {
        return future;
      }
      future = new CompletableFuture<>();
      nextSegmentFutures.put(lastSegment, future);
      lastSegment
          .seal()
          .thenCompose(
              n ->
                  localSegmentManager.nextSegment(
                      meta.getTopicId(),
                      meta.getPartition(),
                      lastSegment.getIndex(),
                      lastSegment.getEndOffset()))
          .thenAccept(
              s -> {
                this.lastSegment = s;
                segmentStartOffsets.put(s.getStartOffset(), s);
                segments.add(s);
                nextSegmentFutures.get(lastSegment).complete(s);
                lastSegmentFuture = nextSegmentFutures.get(lastSegment);
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                nextSegmentFutures.remove(lastSegment);
                return null;
              });
      return future;
    }
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    // fast path
    if (this.lastSegment != null && this.lastSegment.getStartOffset() <= offset) {
      Segment segment = this.lastSegment;
      return segment.get(offset);
    }
    Map.Entry<Long, Segment> entry = segmentStartOffsets.floorEntry(offset);
    if (entry == null) {
      // offset is too small
      return CompletableFuture.completedFuture(null);
    }
    return entry.getValue().get(offset);
  }

  @Override
  public CompletableFuture<Void> open() {
    if (!status.compareAndSet(PartitionStatus.NOOP, PartitionStatus.OPENING)) {
      log.warn("cannot open {} status partition {}", status.get(), meta);
      CompletableFuture.completedFuture(null);
    }

    log.info("partition {} opening...", meta);
    return localSegmentManager
        .getSegments(meta.getTopicId(), meta.getPartition())
        .thenCompose(
            s -> {
              segments = new CopyOnWriteArrayList<>(s);
              if (!segments.isEmpty()) {
                return segments.get(segments.size() - 1).seal();
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenAccept(
            n -> {
              for (int i = 0; i < segments.size(); i++) {
                Segment segment = segments.get(i);
                if (segment.getStartOffset() < segment.getEndOffset()) {
                  segmentStartOffsets.put(segment.getStartOffset(), segment);
                }
              }
              log.info("partition {} opened", meta);
            });
  }

  @Override
  public CompletableFuture<Void> close() {
    status.set(PartitionStatus.SHUTTING);
    log.info("partition {} closing...", meta);
    segments = null;
    if (lastSegment == null) {
      log.info("partition {} closed", meta);
      return CompletableFuture.completedFuture(null);
    }
    return lastSegment
        .seal()
        .thenAccept(
            s -> {
              status.compareAndSet(PartitionStatus.SHUTTING, PartitionStatus.SHUTDOWN);
              log.info("partition {} closed", meta);
            });
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }

  enum PartitionStatus {
    NOOP,
    OPENING,
    OPEN,
    SHUTTING,
    SHUTDOWN
  }
}
