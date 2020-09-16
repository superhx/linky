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
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;

import java.util.concurrent.*;

import static org.superhx.linky.broker.persistence.ChannelFiles.NO_OFFSET;

public class IndexBuilder implements Journal.AppendHook<BatchRecordJournalData>, Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private String storePath;
  private Status status;
  private LocalSegmentManager localSegmentManager;
  private Journal<BatchRecordJournalData> journal;
  private long walSlo;
  private Checkpoint checkpoint;
  private BlockingQueue<WaitingIndex> waitingPutIndexes = new LinkedBlockingQueue<>();
  private ExecutorService putIndexExecutor = Utils.newFixedThreadPool(1, "IndexBuilderPut-");
  private ScheduledExecutorService forceIndexScheduler =
      Utils.newScheduledThreadPool(1, "IndexBuilderForce-");

  public IndexBuilder(String storePath) {
    this.storePath = storePath;
  }

  @Override
  public void init() {
    String checkpointPath = storePath + "/checkpoint.json";
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
      afterAppend(record);
      physicalOffset += record.getSize();
    }
  }

  protected void doPutIndex() {
    for (; ; ) {
      try {
        WaitingIndex waitingIndex = waitingPutIndexes.take();
        Segment segment =
            localSegmentManager.getSegment(
                waitingIndex.getTopicId(),
                waitingIndex.getPartition(),
                waitingIndex.getSegmentIndex());
        if (segment == null) {
          log.info("cannot find segment for index {}", waitingIndex);
          continue;
        }
        segment.putIndex(waitingIndex.getIndex());
        walSlo = waitingIndex.getIndex().getPhysicalOffset() + waitingIndex.getIndex().getSize();
      } catch (InterruptedException e) {
        if (status == Status.SHUTDOWN) {
          break;
        }
      }
    }
  }

  protected void doForeIndex() {
    long walSlo = this.walSlo;
    localSegmentManager.forEach(
        seg -> {
          seg.forceIndex();
        });
    checkpoint.setWalLso(walSlo);
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }

  public void setJournal(Journal journal) {
    this.journal = journal;
  }

  @Override
  public void afterAppend(Journal.Record<BatchRecordJournalData> record) {
    BatchRecord batchRecord = record.getData().getBatchRecord();
    int topicId = batchRecord.getTopicId();
    int partition = batchRecord.getPartition();
    int segmentIndex = batchRecord.getSegmentIndex();
    for (long offset = batchRecord.getFirstOffset();
        offset < batchRecord.getFirstOffset() + batchRecord.getRecordsCount();
        offset++) {
      waitingPutIndexes.offer(
          new WaitingIndex(
              topicId,
              partition,
              segmentIndex,
              new Segment.Index(offset, record.getOffset(), record.getSize())));
    }
  }

  static class WaitingIndex {
    private int topicId;
    private int partition;
    private int segmentIndex;
    private Segment.Index index;

    public WaitingIndex(int topicId, int partition, int segmentIndex, Segment.Index index) {
      this.topicId = topicId;
      this.partition = partition;
      this.segmentIndex = segmentIndex;
      this.index = index;
    }

    public int getTopicId() {
      return topicId;
    }

    public int getPartition() {
      return partition;
    }

    public int getSegmentIndex() {
      return segmentIndex;
    }

    public Segment.Index getIndex() {
      return index;
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
