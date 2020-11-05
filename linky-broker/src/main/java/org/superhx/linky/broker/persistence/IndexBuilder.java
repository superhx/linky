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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;

import java.util.List;
import java.util.concurrent.*;

import static org.superhx.linky.broker.persistence.ChannelFiles.NO_OFFSET;

public class IndexBuilder implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(IndexBuilder.class);
  private String storePath;
  private Status status;
  private Journal journal;
  private ChunkManager chunkManager;
  private long walSlo;
  private Checkpoint checkpoint;
  private BlockingQueue<BatchIndex> waitingPutIndexes = new LinkedBlockingQueue<>();
  private ExecutorService putIndexExecutor = Utils.newFixedThreadPool(1, "IndexBuilderPut-");
  private ScheduledExecutorService forceIndexScheduler =
      Utils.newScheduledThreadPool(1, "IndexBuilderForce-");

  public IndexBuilder(String storePath) {
    this.storePath = storePath;
  }

  @Override
  public void init() {
    String checkpointPath = storePath + "/indexCheckpoint.json";
    checkpoint = Checkpoint.load(checkpointPath);
    walSlo = checkpoint.getWalLso();
    recover();
  }

  @Override
  public void start() {
    status = Status.START;
    putIndexExecutor.submit(() -> doPutIndex());
    forceIndexScheduler.scheduleWithFixedDelay(() -> doForeIndex(), 10, 10, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    status = Status.SHUTDOWN;
    putIndexExecutor.shutdown();
    checkpoint.setWalLso(walSlo);
    checkpoint.persist();
  }

  protected void recover() {
    try {
      recover0();
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    } catch (ExecutionException ex) {
      ex.printStackTrace();
    }
  }

  protected void recover0() throws InterruptedException, ExecutionException {
    if (journal.getStartOffset() == NO_OFFSET) {
      log.info("wal[{}] is empty, skip recover index");
      return;
    }
    for (long physicalOffset = Math.max(walSlo, journal.getStartOffset());
        physicalOffset < journal.getConfirmOffset(); ) {
      Journal.Record record = journal.get(physicalOffset).get();
      if (record.isBlank()) {
        physicalOffset += record.getSize();
        continue;
      }
      BatchRecord batchRecord = null;
      try {
        batchRecord = BatchRecord.parseFrom(record.getData().toByteArray());
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      Builder indexBuilder =
          IndexBuilder.BatchIndex.newBuilder()
              .setTopicId(batchRecord.getTopicId())
              .setPartition(batchRecord.getPartition())
              .setSegmentIndex(batchRecord.getSegmentIndex())
              .setCount(batchRecord.getRecordsCount())
              .setPhysicalOffset(record.getOffset())
              .setSize(record.getSize());
      List<Chunk> chunks =
          chunkManager.getChunks(
              batchRecord.getTopicId(), batchRecord.getPartition(), batchRecord.getSegmentIndex());
      for (int i = chunks.size() - 1; i >= 0; i--) {
        Chunk chunk = chunks.get(i);
        if (chunk.getStartOffset() <= batchRecord.getFirstOffset()) {
          indexBuilder.setChunk(chunk);
          break;
        }
      }
      BatchIndex batchIndex = indexBuilder.build();

      physicalOffset += record.getSize();
      if (batchIndex.getChunk() == null) {
        log.error("cannot find chunk for {}", batchIndex);
        continue;
      }
      putIndex(batchIndex);
    }
  }

  public void putIndex(BatchIndex batchIndex) {
    waitingPutIndexes.offer(batchIndex);
  }

  protected void doPutIndex() {
    for (; ; ) {
      try {
        BatchIndex batchIndex = waitingPutIndexes.take();
        for (long offset = batchIndex.getOffset();
            offset < batchIndex.getOffset() + batchIndex.getCount();
            offset++) {
          batchIndex
              .getChunk()
              .putIndex(new Index(offset, batchIndex.getPhysicalOffset(), batchIndex.getSize()));
        }
        walSlo = batchIndex.getPhysicalOffset() + batchIndex.getSize();
      } catch (InterruptedException e) {
        if (status == Status.SHUTDOWN) {
          break;
        }
      }
    }
  }

  protected void doForeIndex() {
    long walSlo = this.walSlo;
    chunkManager.foreach(
        c -> {
          c.forceIndex();
        });
    checkpoint.setWalLso(walSlo);
  }

  public void setJournal(Journal journal) {
    this.journal = journal;
  }

  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }

  static class BatchIndex {
    private int topicId;
    private int partition;
    private int segmentIndex;
    private long offset;
    private long physicalOffset;
    private int size;
    private int count;
    private Chunk chunk;

    public int getTopicId() {
      return topicId;
    }

    public int getPartition() {
      return partition;
    }

    public int getSegmentIndex() {
      return segmentIndex;
    }

    public long getOffset() {
      return offset;
    }

    public long getPhysicalOffset() {
      return physicalOffset;
    }

    public int getSize() {
      return size;
    }

    public int getCount() {
      return count;
    }

    public Chunk getChunk() {
      return chunk;
    }

    public static Builder newBuilder() {
      return new Builder();
    }
  }

  static class Builder {
    private int topicId;
    private int partition;
    private int segmentIndex;
    private long offset;
    private int count;
    private long physicalOffset;
    private int size;
    private Chunk chunk;

    public Builder setTopicId(int topicId) {
      this.topicId = topicId;
      return this;
    }

    public Builder setPartition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder setSegmentIndex(int segmentIndex) {
      this.segmentIndex = segmentIndex;
      return this;
    }

    public Builder setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder setCount(int count) {
      this.count = count;
      return this;
    }

    public Builder setPhysicalOffset(long physicalOffset) {
      this.physicalOffset = physicalOffset;
      return this;
    }

    public Builder setSize(int size) {
      this.size = size;
      return this;
    }

    public Builder setChunk(Chunk chunk) {
      this.chunk = chunk;
      return this;
    }

    public BatchIndex build() {
      BatchIndex batchIndex = new BatchIndex();
      batchIndex.topicId = topicId;
      batchIndex.partition = partition;
      batchIndex.segmentIndex = segmentIndex;
      batchIndex.offset = offset;
      batchIndex.count = count;
      batchIndex.physicalOffset = physicalOffset;
      batchIndex.size = size;
      batchIndex.chunk = chunk;
      return batchIndex;
    }
  }

  static class Checkpoint {
    private String path;
    private long walLso;

    public long getWalLso() {
      return walLso;
    }

    public void setWalLso(long walLso) {
      this.walLso = walLso;
    }

    public void persist() {
      Utils.jsonObj2file(this, path);
    }

    public static Checkpoint load(String path) {
      Checkpoint checkpoint = Utils.file2jsonObj(path, Checkpoint.class);
      if (checkpoint == null) {
        checkpoint = new Checkpoint();
      }
      checkpoint.path = path;
      return checkpoint;
    }
  }
}
