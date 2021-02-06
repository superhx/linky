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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class LocalPartitionImpl implements Partition {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private PartitionMeta meta;
  private AtomicReference<PartitionStatus> status = new AtomicReference<>(PartitionStatus.NOOP);
  private List<Segment> segments;
  private volatile Segment lastSegment;
  private LocalSegmentManager localSegmentManager;
  private CompletableFuture<Segment> lastSegmentFuture;
  private Map<Segment, CompletableFuture<Segment>> nextSegmentFutures = new ConcurrentHashMap<>();

  public LocalPartitionImpl(PartitionMeta meta) {
    this.meta = meta;
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    ByteBuffer cursor = ByteBuffer.allocate(4 + 8);
    return getLastSegment()
        .thenCompose(
            s -> {
              cursor.putInt(s.getIndex());
              switch (s.getStatus()) {
                case WRITABLE:
                case REPLICA_LOSS:
                  return s.append(batchRecord);
                case REPLICA_BREAK:
                  return nextSegment(s).thenCompose(n -> n.append(batchRecord));
              }
              throw new LinkyIOException(String.format("Unknown status %s", s.getStatus()));
            })
        .thenApply(
            appendResult -> {
              cursor.putLong(appendResult.getOffset());
              return new AppendResult(cursor.array());
            });
  }

  protected CompletableFuture<Segment> getLastSegment() {
    if (lastSegmentFuture != null) {
      return lastSegmentFuture;
    }
    synchronized (this) {
      if (lastSegmentFuture != null) {
        return lastSegmentFuture;
      }

      lastSegmentFuture = new CompletableFuture();
      localSegmentManager
          .nextSegment(
              meta.getTopicId(),
              meta.getPartition(),
              segments.isEmpty() ? Segment.NO_INDEX : segments.get(segments.size() - 1).getIndex())
          .thenAccept(
              s -> {
                lastSegmentFuture.complete(s);
                this.lastSegment = s;
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
                      meta.getTopicId(), meta.getPartition(), lastSegment.getIndex()))
          .thenAccept(
              s -> {
                this.lastSegment = s;
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
  public CompletableFuture<GetResult> get(byte[] cursor) {
    ByteBuffer cursorBuf = ByteBuffer.wrap(cursor);
    int segmentIndex = cursorBuf.getInt();
    long offset = cursorBuf.getLong();

    GetResult getResult = new GetResult();
    CompletableFuture<BatchRecord> recordFuture;
    Segment segment = this.lastSegment;
    if (segment != null && segment.getIndex() != segmentIndex) {
      for (Segment seg : segments) {
        if (seg.getIndex() == segmentIndex) {
          if (seg.getEndOffset() != Segment.NO_OFFSET && seg.getEndOffset() <= offset) {
            segmentIndex++;
            offset = 0;
            continue;
          }
          segment = seg;
        }
      }
    }
    if (segment != null) {
      recordFuture = segment.get(offset);
    } else {
      recordFuture = CompletableFuture.completedFuture(null);
    }
    ByteBuffer nextCursor = ByteBuffer.allocate(4 + 8);
    nextCursor.putInt(segmentIndex);
    long finalOffset = offset;
    return recordFuture.thenApply(
        r -> {
          if (r == null) {
            getResult.setStatus(GetStatus.NO_NEW_MSG);
          } else {
            getResult.setBatchRecord(r);
            nextCursor.putLong(finalOffset + r.getRecordsCount());
          }
          getResult.setNextCursor(nextCursor.array());
          return getResult;
        });
  }

  @Override
  public CompletableFuture<PartitionStatus> open() {
    if (!status.compareAndSet(PartitionStatus.NOOP, PartitionStatus.OPENING)) {
      log.warn("cannot open {} status partition {}", status.get(), meta);
      return CompletableFuture.completedFuture(this.status.get());
    }

    log.info("partition {} opening...", meta);
    CompletableFuture<PartitionStatus> openFuture = new CompletableFuture<>();
    localSegmentManager
        .getSegments(meta.getTopicId(), meta.getPartition())
        .thenCompose(
            s -> {
              segments = new CopyOnWriteArrayList<>(s);
              Collections.sort(segments, Comparator.comparingInt(Segment::getIndex));
              if (!segments.isEmpty()) {
                return segments.get(segments.size() - 1).seal();
              }
              return CompletableFuture.completedFuture(null);
            })
        .thenAccept(
            n -> {
              this.status.set(PartitionStatus.OPEN);
              log.info("partition {} opened", meta);
              openFuture.complete(this.status.get());
            })
        .exceptionally(
            t -> {
              log.warn("partition {} open fail", meta, t);
              close()
                  .handle(
                      (r, ct) -> {
                        openFuture.complete(this.status.get());
                        return null;
                      });
              return null;
            });
    return openFuture;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    status.set(PartitionStatus.SHUTTING);
    log.info("partition {} closing...", meta);
    segments = null;
    if (lastSegment == null) {
      log.info("partition {} closed", meta);
      closeFuture.complete(null);
      return closeFuture;
    }
    lastSegment
        .seal()
        .handle(
            (nil, t) -> {
              if (t != null) {
                log.warn("partition {} close fail", meta, t);
              }
              status.compareAndSet(PartitionStatus.SHUTTING, PartitionStatus.SHUTDOWN);
              log.info("partition {} closed", meta);
              closeFuture.complete(null);
              return null;
            });
    return closeFuture;
  }

  @Override
  public PartitionStatus status() {
    return this.status.get();
  }

  @Override
  public PartitionMeta meta() {
    return this.meta;
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }
}
