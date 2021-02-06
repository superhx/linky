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
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.loadbalance.SegmentKey;
import org.superhx.linky.service.proto.ChunkMeta;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ChunkManager implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(LocalChunk.class);
  private AtomicInteger atomicChunkId = new AtomicInteger();

  private List<LinkyData> linkyDatas = new ArrayList<>();
  private AtomicInteger roundRobin = new AtomicInteger();
  private Map<SegmentKey, List<Chunk>> segmentChunksMap = new ConcurrentHashMap<>();
  private Map<Journal, List<Chunk>> journal2Chunks = new ConcurrentHashMap<>();
  private JournalManager journalManager;
  private IndexerManager indexerManager;
  private PersistentMeta persistentMeta;

  public ChunkManager() {}

  @Override
  public void init() {
    journalManager
        .journals()
        .forEach(
            (path, journal) -> {
              journal2Chunks.put(journal, new CopyOnWriteArrayList<>());
              linkyDatas.add(new LinkyData(path, journal, indexerManager.getIndexBuilder(path)));
            });

    int chunkIdMax = 0;
    try (RocksIterator it = persistentMeta.chunkMetaIterator()) {
      for (; it.isValid(); it.next()) {
        int chunkId = ByteBuffer.wrap(it.key()).getInt();
        chunkIdMax = Math.max(chunkIdMax, chunkId);
        try {
          ChunkMeta meta = ChunkMeta.parseFrom(it.value());
          Journal journal = journalManager.journal(meta.getPath());
          Indexer indexer = indexerManager.getIndexBuilder(meta.getPath());
          LocalChunk chunk = new LocalChunk(meta);
          chunk.setJournal(journal);
          chunk.setIndexer(indexer);
          chunk.init();
          SegmentKey segmentKey =
              new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getSegmentIndex());
          List<Chunk> chunks = segmentChunksMap.get(segmentKey);
          if (chunks == null) {
            chunks = new ArrayList<>(1);
            segmentChunksMap.put(segmentKey, chunks);
          }
          chunks.add(chunk);
          journal2Chunks.get(journal).add(chunk);
        } catch (InvalidProtocolBufferException e) {
          log.error("[CHUNK_META_CORRUPT] unknown chunkid[{}] meta[{}]", chunkId, it.value(), e);
          continue;
        }
      }
    }
    atomicChunkId.set(chunkIdMax);
    segmentChunksMap
        .values()
        .forEach(l -> Collections.sort(l, Comparator.comparingLong(Chunk::startOffset)));
  }

  @Override
  public void start() {
    foreach(c -> c.init());
  }

  @Override
  public void shutdown() {
    foreach(c -> c.shutdown());
    segmentChunksMap.clear();
  }

  public synchronized List<Chunk> getChunks(int topicId, int partition, int segmentIndex) {
    return Optional.ofNullable(
            segmentChunksMap.get(new SegmentKey(topicId, partition, segmentIndex)))
        .orElse(Collections.emptyList());
  }

  public Map<Journal, List<Chunk>> getChunks() {
    return journal2Chunks;
  }

  public synchronized Chunk newChunk(ChunkMeta meta) {
    ChunkMeta.Builder metaBuilder = meta.toBuilder();
    int chunkId = atomicChunkId.incrementAndGet();
    metaBuilder.setChunkId(chunkId);
    LinkyData linkyData =
        linkyDatas.get(Math.abs(roundRobin.incrementAndGet() % linkyDatas.size()));
    metaBuilder.setPath(linkyData.getPath());
    meta = metaBuilder.build();
    LocalChunk chunk = new LocalChunk(meta);
    chunk.setJournal(linkyData.getJournal());
    chunk.setIndexer(linkyData.getIndexer());
    persistentMeta.putChunkMeta(meta);

    chunk.init();
    chunk.start();

    SegmentKey segmentKey =
        new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getSegmentIndex());
    List<Chunk> chunks = segmentChunksMap.get(segmentKey);
    if (chunks == null) {
      chunks = new ArrayList<>(1);
      segmentChunksMap.put(segmentKey, chunks);
    }
    chunks.add(chunk);
    journal2Chunks.get(linkyData.getJournal()).add(chunk);
    return chunk;
  }

  public void foreach(Consumer<Chunk> consumer) {
    segmentChunksMap.values().stream().forEach(l -> l.forEach(consumer));
  }

  public void setJournalManager(JournalManager journalManager) {
    this.journalManager = journalManager;
  }

  public void setIndexerManager(IndexerManager indexerManager) {
    this.indexerManager = indexerManager;
  }

  public void setPersistentMeta(PersistentMeta persistentMeta) {
    this.persistentMeta = persistentMeta;
  }

  static class LinkyData {
    private String path;
    private Journal journal;
    private Indexer indexer;

    public LinkyData(String path, Journal journal, Indexer indexer) {
      this.path = path;
      this.journal = journal;
      this.indexer = indexer;
    }

    public String getPath() {
      return path;
    }

    public Journal getJournal() {
      return journal;
    }

    public Indexer getIndexer() {
      return indexer;
    }
  }
}
