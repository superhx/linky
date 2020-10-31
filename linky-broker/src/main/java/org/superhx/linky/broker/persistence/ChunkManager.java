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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkManager implements Lifecycle {
  private LinkyData linkyData;
  private Map<String, Chunk> chunkMap = new ConcurrentHashMap<>();

  public ChunkManager(String path, JournalManager journalManager) {
    String dataDirPath = path + "/data";
    Utils.ensureDirOK(dataDirPath);
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
        Journal<BatchRecordJournalData> journal = journalManager.getJournal(linky.getPath());
        this.linkyData = new LinkyData(linky.getPath(), journal);
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
          Chunk chunk = new LocalChunk(chunkDir.getPath(), chunkDir.getName(), journal);
          chunkMap.put(chunk.name(), chunk);
        }
      }
    }
  }

  public List<Chunk> getChunksByPrefix(String prefix) {
    return null;
  }

  public Chunk newChunk(String name) {
    Chunk chunk = new LocalChunk(linkyData.getPath() + "/chunks", name, linkyData.getJournal());
    chunk.init();
    chunk.start();
    return chunk;
  }

  static class LinkyData {
    private String path;
    private Journal<BatchRecordJournalData> journal;

    public LinkyData(String path, Journal<BatchRecordJournalData> journal) {
      this.path = path;
      this.journal = journal;
    }

    public String getPath() {
      return path;
    }

    public Journal<BatchRecordJournalData> getJournal() {
      return journal;
    }
  }
}
