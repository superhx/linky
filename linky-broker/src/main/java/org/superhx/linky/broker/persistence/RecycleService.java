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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RecycleService {
  private static final Logger log = LoggerFactory.getLogger(RecycleService.class);
  private Map<Chunk, AtomicLong> chunkReclaimOffsets;
  private ChunkManager chunkManager;

  private void scan() {
    // calculate file recyclable size
    // 也需要能从中间回收，例如压测的流量就需要从中间开始回收，journal 提供一个 compact(startFile, endFile, msg -> {}) 的接口
    Map<Journal, List<Chunk>> journal2Chunks = chunkManager.getChunks();
  }

  private void recycle(Journal journal) {}

  class JournalStats {
    private Journal journal;
    private Map<JournalLog, AtomicLong> recycableSize = new HashMap<>();

    private void scan() {
      List<Chunk> chunks = chunkManager.getChunks().get(journal);
      for (Chunk chunk : chunks) {
        long reclaimOffset = chunk.getReclaimOffset();
        if (reclaimOffset <= 0) {
          continue;
        }
        AtomicLong lastReclaimOffset = chunkReclaimOffsets.get(chunk);
        if (lastReclaimOffset == null) {
          chunkReclaimOffsets.put(chunk, new AtomicLong(chunk.getStartOffset()));
        }
        if (lastReclaimOffset.get() == reclaimOffset) {
          continue;
        }
        Iterator<Index> it = chunk.indexIterator(lastReclaimOffset.get(), reclaimOffset);
        while (it.hasNext()) {
          Index index = it.next();
          markRecyclable(index.getPhysicalOffset(), index.getPhysicalOffset() + index.getSize());
        }
      }
    }

    private void markRecyclable(long startPhysicalOffset, long endPhysicalOffset) {
      JournalLog journalLog = journal.getJournalLog(startPhysicalOffset);
      if (journalLog == null) {
        log.warn(
            "[MARK_RECYCLABLE_FAIL] cannot find journal log by offset {}", startPhysicalOffset);
      }
      AtomicLong size = recycableSize.get(journalLog);
      if (size == null) {
        size = new AtomicLong();
        recycableSize.put(journalLog, size);
      }
      size.addAndGet(endPhysicalOffset - startPhysicalOffset);
    }

    private void recycle() {

    }
  }

  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }
}
