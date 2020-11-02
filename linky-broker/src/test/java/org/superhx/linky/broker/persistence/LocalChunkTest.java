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

import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.superhx.linky.broker.Configuration;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.Record;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class LocalChunkTest {
  private static final String path = System.getProperty("user.home") + "/linkytest/localchunktest";
  private Chunk chunk;
  private Journal journal;

  @Before
  public void setup() {
    chunk = new LocalChunk(path + "/chunks", 0, 1, 2, 200);
    ((LocalChunk) chunk).setIndexBuilder(new IndexBuilderMock(chunk));
    journal = new JournalImpl(path + "/logs", new Configuration());
    journal.init();
    journal.start();
    ((LocalChunk) chunk).setJournal(journal);
    chunk.init();
    chunk.start();
  }

  @After
  public void teardown() {
    chunk.shutdown();
    journal.shutdown();
    journal.delete();
    chunk.delete();
  }

  @Test
  public void test_appendAndGet() throws InterruptedException, ExecutionException {
    int count = 2048;
    CountDownLatch latch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      BatchRecord.Builder batchRecord =
          BatchRecord.newBuilder()
              .setTopicId(0)
              .setPartition(1)
              .setSegmentIndex(2)
              .setFirstOffset(200 + 2 * i);
      batchRecord.addRecords(
          Record.newBuilder().setKey("hello").setValue(ByteString.copyFromUtf8("world" + i)));
      batchRecord.addRecords(
          Record.newBuilder().setKey("hello").setValue(ByteString.copyFromUtf8("world" + (i + 1))));
      chunk.append(batchRecord.build()).thenAccept(nil -> latch.countDown());
    }
    latch.await();
    for (int i = 0; i < count * 2; i++) {
      BatchRecord batchRecord = chunk.get(200 + i).get();
      Assert.assertEquals(batchRecord.getFirstOffset() / 2 * 2, batchRecord.getFirstOffset());
      Assert.assertEquals((200 + i) / 2 * 2, batchRecord.getFirstOffset() / 2 * 2);
      Assert.assertEquals(0, batchRecord.getTopicId());
      Assert.assertEquals(1, batchRecord.getPartition());
      Assert.assertEquals(2, batchRecord.getSegmentIndex());
      Assert.assertEquals(2, batchRecord.getRecordsCount());
      Assert.assertEquals("hello", batchRecord.getRecords(0).getKey());
      Assert.assertEquals("hello", batchRecord.getRecords(1).getKey());
      Assert.assertEquals("world" + (i / 2), batchRecord.getRecords(0).getValue().toStringUtf8());
      Assert.assertEquals(
          "world" + (i / 2 + 1), batchRecord.getRecords(1).getValue().toStringUtf8());
    }
  }

  class IndexBuilderMock extends IndexBuilder {
    private Chunk chunk;

    public IndexBuilderMock(Chunk chunk) {
      super("");
      this.chunk = chunk;
    }

    @Override
    public void putIndex(BatchIndex batchIndex) {
      for (long offset = batchIndex.getOffset();
          offset < batchIndex.getOffset() + batchIndex.getCount();
          offset++) {
        chunk.putIndex(new Index(offset, batchIndex.getPhysicalOffset(), batchIndex.getSize()));
      }
    }
  }
}
