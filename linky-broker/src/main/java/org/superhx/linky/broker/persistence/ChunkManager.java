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

import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.loadbalance.SegmentKey;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class ChunkManager implements Lifecycle {
  private String path;
  private LinkyData linkyData;
  private Map<SegmentKey, List<Chunk>> segmentChunksMap = new ConcurrentHashMap<>();
  private JournalManager journalManager;
  private IndexBuilderManager indexBuilderManager;

  public ChunkManager(String path) {
    this.path = path;
    String dataDirPath = path + "/data";
    Utils.ensureDirOK(dataDirPath);
  }

  @Override
  public void init() {
    String dataDirPath = path + "/data";
    File dataDir = new File(dataDirPath);
    File[] linkys = dataDir.listFiles();
    if (linkys == null) {
      throw new LinkyIOException(String.format("%s is not directory", dataDirPath));
    }
    for (File linky : linkys) {
      if (linky.isFile()) {
        continue;
      }
      if (linky.getName().startsWith("linky")) {
        Journal journal = journalManager.getJournal(linky.getPath());
        IndexBuilder indexBuilder = indexBuilderManager.getIndexBuilder(linky.getPath());
        this.linkyData = new LinkyData(linky.getPath(), journal, indexBuilder);
        String chunksDirPath = linky.getPath() + "/chunks";
        Utils.ensureDirOK(chunksDirPath);
        File chunksDir = new File(chunksDirPath);
        File[] chunkDirs = chunksDir.listFiles();
        if (chunkDirs == null) {
          throw new LinkyIOException(String.format("%s is not directory", chunksDirPath));
        }
        for (File chunkDir : chunkDirs) {
          if (chunkDir.isFile() || !chunkDir.getName().startsWith("chunk.")) {
            continue;
          }
          String[] parts = chunkDir.getName().split("@");
          int topicId = Integer.valueOf(parts[0]);
          int partition = Integer.valueOf(parts[1]);
          int segmentIndex = Integer.valueOf(parts[2]);
          long startOffset = Integer.valueOf(parts[3]);

          Chunk chunk =
              new LocalChunk(chunkDir.getPath(), topicId, partition, segmentIndex, startOffset);
          ((LocalChunk) chunk).setJournal(journal);
          ((LocalChunk) chunk).setIndexBuilder(indexBuilder);
          chunk.init();
          SegmentKey segmentKey = new SegmentKey(topicId, partition, segmentIndex);
          List<Chunk> chunks = segmentChunksMap.get(segmentKey);
          if (chunks == null) {
            chunks = new ArrayList<>(1);
            segmentChunksMap.put(segmentKey, chunks);
          }
          chunks.add(chunk);
        }
      }
    }
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

  public List<Chunk> getChunks(int topicId, int partition, int segmentIndex) {
    return Optional.ofNullable(
            segmentChunksMap.get(new SegmentKey(topicId, partition, segmentIndex)))
        .orElse(Collections.emptyList());
  }

  public Chunk newChunk(int topicId, int partition, int segmentIndex, long startOffset) {
    Chunk chunk =
        new LocalChunk(
            linkyData.getPath() + "/chunks", topicId, partition, segmentIndex, startOffset);
    ((LocalChunk) chunk).setJournal(linkyData.getJournal());
    ((LocalChunk) chunk).setIndexBuilder(linkyData.getIndexBuilder());
    chunk.init();
    chunk.start();
    return chunk;
  }

  public void foreach(Consumer<Chunk> consumer) {
    segmentChunksMap.values().stream().forEach(l -> l.forEach(consumer));
  }

  public void setJournalManager(JournalManager journalManager) {
    this.journalManager = journalManager;
  }

  public void setIndexBuilderManager(IndexBuilderManager indexBuilderManager) {
    this.indexBuilderManager = indexBuilderManager;
  }

  static class LinkyData {
    private String path;
    private Journal journal;
    private IndexBuilder indexBuilder;

    public LinkyData(String path, Journal journal, IndexBuilder indexBuilder) {
      this.path = path;
      this.journal = journal;
      this.indexBuilder = indexBuilder;
    }

    public String getPath() {
      return path;
    }

    public Journal getJournal() {
      return journal;
    }

    public IndexBuilder getIndexBuilder() {
      return indexBuilder;
    }
  }
}
