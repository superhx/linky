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
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.superhx.linky.broker.persistence.ChannelFiles.NO_OFFSET;

public class Indexer implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(Indexer.class);
  private static final String CHUNK_INDEX_COLUMN_FAMILY = "CI";
  private static final byte[] WAL_SLO_KEY = "walSlo".getBytes(Utils.DEFAULT_CHARSET);

  static {
    RocksDB.loadLibrary();
  }

  private Status status;
  private Journal journal;
  private ChunkManager chunkManager;
  private long walSlo;
  private BlockingQueue<BatchIndex> waitingPutIndexes = new LinkedBlockingQueue<>();
  private ExecutorService putIndexExecutor = Utils.newFixedThreadPool(1, "IndexBuilderPut-");
  private ScheduledExecutorService forceIndexScheduler =
      Utils.newScheduledThreadPool(1, "IndexBuilderForce-");
  private RocksDB rocksDB;
  private ColumnFamilyHandle chunkIndexFamilyHandle;

  public Indexer(String storePath) {
    Utils.ensureDirOK(storePath);
    DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
            new ColumnFamilyDescriptor(CHUNK_INDEX_COLUMN_FAMILY.getBytes(), cfOptions));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try {
      rocksDB = RocksDB.open(options, storePath, cfDescriptors, columnFamilyHandleList);
      chunkIndexFamilyHandle = columnFamilyHandleList.get(1);
      byte[] walSloBytes = rocksDB.get(WAL_SLO_KEY);
      if (walSloBytes != null) {
        walSlo = ByteBuffer.wrap(walSloBytes).getLong();
      }
    } catch (RocksDBException e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public void init() {
    putIndexExecutor.submit(
        () -> {
          try {
            doPutIndex();
          } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
          }
        });
    forceIndexScheduler.scheduleWithFixedDelay(() -> doForeIndex(), 10, 10, TimeUnit.MILLISECONDS);
    recover();
  }

  @Override
  public void start() {
    status = Status.START;
  }

  @Override
  public void shutdown() {
    status = Status.SHUTDOWN;
    putIndexExecutor.shutdown();
    doForeIndex();
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
          Indexer.BatchIndex.newBuilder()
              .setTopicId(batchRecord.getTopicId())
              .setPartition(batchRecord.getPartition())
              .setSegmentIndex(batchRecord.getSegmentIndex())
              .setCount(batchRecord.getRecordsCount())
              .setPhysicalOffset(record.getOffset())
              .setSize(record.getSize())
              .setTerm(batchRecord.getTerm());
      List<Chunk> chunks =
          chunkManager.getChunks(
              batchRecord.getTopicId(), batchRecord.getPartition(), batchRecord.getSegmentIndex());
      for (int i = chunks.size() - 1; i >= 0; i--) {
        Chunk chunk = chunks.get(i);
        if (chunk.startOffset() <= batchRecord.getFirstOffset()) {
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

  public BatchIndex getIndex(Chunk chunk, long offset) {
    BatchIndex batchIndex = new BatchIndex();
    batchIndex.chunk = chunk;
    batchIndex.offset = offset;
    byte[] keyLeftBound = indexKey(batchIndex);
    RocksIterator it = rocksDB.newIterator(chunkIndexFamilyHandle, new ReadOptions());
    it.seek(keyLeftBound);
    for (; it.isValid(); it.next()) {
      byte[] key = it.key();
      for (int i = 0; i < 4 + 8; i++) {
        if (keyLeftBound[i] != key[i]) {
          return null;
        }
      }
      byte[] value = it.value();
      ByteBuffer buffer = ByteBuffer.wrap(value);
      batchIndex.physicalOffset = buffer.getLong();
      batchIndex.size = buffer.getInt();
      return batchIndex;
    }
    return null;
  }

  public BatchIndex getLastIndex(Chunk chunk) {
    BatchIndex batchIndex = new BatchIndex();
    batchIndex.chunk = chunk;
    batchIndex.offset = Long.MAX_VALUE;
    byte[] keyEndBound = indexKey(batchIndex);
    RocksIterator it = rocksDB.newIterator(chunkIndexFamilyHandle, new ReadOptions());
    it.seekForPrev(keyEndBound);
    if (!it.isValid()) {
      return null;
    }
    byte[] key = it.key();
    for (int i = 0; i < 4; i++) {
      if (keyEndBound[i] != key[i]) {
        return null;
      }
    }
    fillKey(batchIndex, it.key());
    fillValue(batchIndex, it.value());
    return batchIndex;
  }

  static void fillKey(BatchIndex index, byte[] key) {
    ByteBuffer buffer = ByteBuffer.wrap(key);
    buffer.getInt();
    index.offset = buffer.getLong();
    index.count = buffer.getInt();
    index.term = buffer.getInt();
  }

  static void fillValue(BatchIndex index, byte[] value) {
    ByteBuffer buffer = ByteBuffer.wrap(value);
    index.physicalOffset = buffer.getLong();
    index.size = buffer.getInt();
  }

  protected void doPutIndex() throws RocksDBException {
    for (; ; ) {
      try {
        BatchIndex batchIndex = waitingPutIndexes.take();
        WriteBatch writeBatch = new WriteBatch();
        byte[] key = indexKey(batchIndex);
        byte[] value = indexValue(batchIndex);
        writeBatch.put(chunkIndexFamilyHandle, key, value);
        rocksDB.write(new WriteOptions(), writeBatch);
        walSlo = batchIndex.getPhysicalOffset() + batchIndex.getSize();
      } catch (InterruptedException ex) {
        if (status == Status.SHUTDOWN) {
          break;
        }
      }
    }
  }

  static byte[] indexKey(BatchIndex index) {
    ByteBuffer buffer = ByteBuffer.allocate(4 + 8 + 4 + 4);
    buffer.putInt(index.getChunk().chunkId());
    buffer.putLong(index.getOffset());
    buffer.putInt(index.getCount());
    buffer.putInt(index.getTerm());
    return buffer.array();
  }

  static byte[] indexValue(BatchIndex index) {
    ByteBuffer buffer = ByteBuffer.allocate(8 + 4);
    buffer.putLong(index.getPhysicalOffset());
    buffer.putInt(index.getSize());
    return buffer.array();
  }

  protected void doForeIndex() {
    long walSlo = this.walSlo;
    try {
      rocksDB.flush(new FlushOptions().setWaitForFlush(true));
      ByteBuffer walSloBytes = ByteBuffer.allocate(8);
      walSloBytes.putLong(walSlo);
      rocksDB.put(WAL_SLO_KEY, walSloBytes.array());
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
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
    private int term;
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

    public int getTerm() {
      return term;
    }

    public void setTerm(int term) {
      this.term = term;
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
    private int term;
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

    public int getTerm() {
      return term;
    }

    public Builder setTerm(int term) {
      this.term = term;
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
      batchIndex.term = term;
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
