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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class LocalChunk implements Chunk {
  private static final int INDEX_UNIT_SIZE = 12;
  private static final int FILE_SIZE = 12 * 1024;
  private static final Logger log = LoggerFactory.getLogger(LocalChunk.class);
  private String storePath;
  private String name;
  private long startOffset;
  private Journal journal;
  private IndexBuilder indexBuilder;
  private IFiles indexes;

  public LocalChunk(String path, int topicId, int partition, int segmentIndex, long startOffset) {
    this.name = topicId + "@" + partition + "@" + segmentIndex + "@" + startOffset;
    this.startOffset = startOffset;

    this.storePath = String.format("%s/chunk.%s", path, name);
    this.indexes =
        new MappedFiles(
            this.storePath,
            "index",
            FILE_SIZE,
            (indexFile, lso) -> {
              ByteBuffer index = indexFile.read(lso, INDEX_UNIT_SIZE);
              long offset = index.getLong();
              int size = index.getInt();
              if (size == 0) {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return noop", indexFile, lso);
                }
                return lso;
              } else {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return index {}/{}", indexFile, lso, offset, size);
                }
              }
              return lso + 12;
            },
            null);
  }

  @Override
  public void init() {
    indexes.init();
  }

  @Override
  public void start() {
    indexes.start();
  }

  @Override
  public void shutdown() {
    indexes.shutdown();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public CompletableFuture<Void> append(BatchRecord batchRecord) {
    Journal.BytesData bytesData = new Journal.BytesData(batchRecord.toByteArray());
    CompletableFuture<Void> cf = new CompletableFuture<>();
    IndexBuilder.Builder index =
        IndexBuilder.BatchIndex.newBuilder()
            .setTopicId(batchRecord.getTopicId())
            .setPartition(batchRecord.getPartition())
            .setSegmentIndex(batchRecord.getSegmentIndex())
            .setOffset(batchRecord.getFirstOffset())
            .setCount(batchRecord.getRecordsCount())
            .setChunk(this);
    journal.append(
        bytesData,
        r -> {
          indexBuilder.putIndex(
              index.setPhysicalOffset(r.getOffset()).setSize(r.getSize()).build());
          cf.complete(null);
        });
    return cf;
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset) {
    long relativeOffset = offset - startOffset;
    return indexes
        .read(relativeOffset * 12, 12)
        .thenCompose(
            index -> {
              long physicalOffset = index.getLong();
              int size = index.getInt();
              return journal.get(physicalOffset, size);
            })
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
  public long getStartOffset() {
    return startOffset;
  }

  @Override
  public long getConfirmOffset() {
    return startOffset + indexes.confirmOffset() / INDEX_UNIT_SIZE;
  }

  @Override
  public void putIndex(Index index) {
    long expectNextOffset = startOffset + indexes.writeOffset() / INDEX_UNIT_SIZE;
    if (expectNextOffset != index.getOffset()) {
      log.warn("{} {} not match expect nextOffset {}", name, index, expectNextOffset);
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("put {} {}", name, index);
    }
    ByteBuffer buf = ByteBuffer.allocate(12);
    buf.putLong(index.getPhysicalOffset());
    buf.putInt(index.getSize());
    buf.flip();
    indexes.append(buf);
  }

  @Override
  public void forceIndex() {
    indexes.force();
  }

  @Override
  public void delete() {
    indexes.delete();
  }

  public void setJournal(Journal journal) {
    this.journal = journal;
  }

  public void setIndexBuilder(IndexBuilder indexBuilder) {
    this.indexBuilder = indexBuilder;
  }
}
