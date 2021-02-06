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
import java.util.HashMap;
import java.util.Map;

public class IndexerManager implements Lifecycle {
  private Map<String, Indexer> indexBuilderMap = new HashMap<>();
  private JournalManager journalManager;
  private ChunkManager chunkManager;

  public IndexerManager(String path) {
    String dataDirPath = path + "/data";
    Utils.ensureDirOK(dataDirPath + "/linky");
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
        String journalPath = linky.getPath();
        Indexer indexer = new Indexer(journalPath + "/index");
        indexBuilderMap.put(linky.getPath(), indexer);
      }
    }
  }

  @Override
  public void init() {
    for (String path : indexBuilderMap.keySet()) {
      Indexer indexer = indexBuilderMap.get(path);
      indexer.setJournal(journalManager.journal(path));
      indexer.setChunkManager(chunkManager);
      indexer.init();
    }
  }

  @Override
  public void start() {
      indexBuilderMap.values().forEach(i -> i.start());
  }

  @Override
  public void shutdown() {
    indexBuilderMap.values().forEach(i -> i.shutdown());
  }

  public Indexer getIndexBuilder(String path) {
    return indexBuilderMap.get(path);
  }

  public void setJournalManager(JournalManager journalManager) {
    this.journalManager = journalManager;
  }

  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }
}
