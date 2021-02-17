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
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.ChunkMeta;

import java.util.concurrent.CompletableFuture;

public class LocalChunk implements Chunk {
  private static final Logger log = LoggerFactory.getLogger(LocalChunk.class);
  private ChunkMeta meta;
  private Journal journal;
  private Indexer indexer;
  private long startOffset;

  public LocalChunk(ChunkMeta meta) {
    this.meta = meta;
    this.startOffset = meta.getStartOffset();
  }

  @Override
  public void init() {}

  @Override
  public void start() {}

  @Override
  public void shutdown() {}

  @Override
  public int chunkId() {
    return meta.getChunkId();
  }

  @Override
  public CompletableFuture<Void> append(BatchRecord batchRecord) {
    Journal.BytesData bytesData = new Journal.BytesData(batchRecord.toByteArray());
    CompletableFuture<Void> cf = new CompletableFuture<>();
    Indexer.Builder index =
        Indexer.BatchIndex.newBuilder()
            .setTopicId(batchRecord.getTopicId())
            .setPartition(batchRecord.getPartition())
            .setSegmentIndex(batchRecord.getIndex())
            .setOffset(batchRecord.getFirstOffset())
            .setCount(batchRecord.getRecordsCount())
            .setFlag(batchRecord.getFlag())
            .setChunk(this);
    batchRecord.getRecordsList().forEach(r -> index.addKey(r.getKey().toByteArray()));
    journal.append(
        bytesData,
        r -> {
          indexer.putIndex(index.setPhysicalOffset(r.getOffset()).setSize(r.getSize()).build());
          cf.complete(null);
        });
    return cf;
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    Indexer.BatchIndex batchIndex = indexer.getIndex(this, offset);
    if (batchIndex == null) {
      return CompletableFuture.completedFuture(null);
    }
    return journal
        .get(batchIndex.getPhysicalOffset(), batchIndex.getSize())
        .thenApply(
            record -> {
              try {
                return BatchRecord.parseFrom(record.getData().toByteArray());
              } catch (InvalidProtocolBufferException e) {
                throw new LinkyIOException(e);
              }
            });
  }

  @Override
  public CompletableFuture<BatchRecord> getKV(byte[] key, boolean meta) {
    long offset = indexer.getKV(this, key, meta);
    if (offset == Constants.NOOP_OFFSET) {
      return CompletableFuture.completedFuture(null);
    }
    return get(offset);
  }

  @Override
  public long startOffset() {
    return startOffset;
  }

  @Override
  public long getConfirmOffset() {
    Indexer.BatchIndex batchIndex = indexer.getLastIndex(this);
    if (batchIndex == null) {
      return 0;
    }
    return batchIndex.getOffset() + batchIndex.getCount();
  }

  //
  //  @Override
  //  public long getReclaimOffset() {
  //    return reclaimOffset;
  //  }

  //  @Override
  //  public Iterator<Index> indexIterator(long startOffset, long endOffset) {
  //    return new IndexIterator(startOffset, endOffset);
  //  }

  public void setJournal(Journal journal) {
    this.journal = journal;
  }

  public void setIndexer(Indexer indexer) {
    this.indexer = indexer;
  }

  //  class IndexIterator implements Iterator<Index> {
  //    private long startOffset;
  //    private long endOffset;
  //    private long currentOffset;
  //
  //    public IndexIterator(long startOffset, long endOffset) {
  //      this.startOffset = startOffset;
  //      this.endOffset = endOffset;
  //      this.currentOffset = startOffset - 1;
  //    }
  //
  //    @Override
  //    public boolean hasNext() {
  //      long offset = currentOffset + 1;
  //      return offset < endOffset && offset < getConfirmOffset();
  //    }
  //
  //    @Override
  //    public Index next() {
  //      long offset = ++currentOffset;
  //      long relativeOffset = offset - startOffset;
  //      try {
  //        ByteBuffer buffer = indexes.read(relativeOffset * 12, 12).get();
  //        long physicalOffset = buffer.getLong();
  //        int size = buffer.getInt();
  //        return new Index(offset, physicalOffset, size);
  //      } catch (Exception e) {
  //        throw new LinkyIOException(e);
  //      }
  //    }
  //  }
}
