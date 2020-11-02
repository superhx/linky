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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.superhx.linky.broker.Configuration;
import org.superhx.linky.broker.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class JournalTest {
  private static final String path = System.getProperty("user.home") + "/linkytest/journaltest";
  private Journal journal;

  @Before
  public void setup() {
    Configuration configuration = new Configuration();
    configuration.put(JournalImpl.GROUP_APPEND_BATCH_SIZE_KEY, 256);
    journal = new JournalImpl(path, configuration);
    journal.init();
  }

  @After
  public void teardown() {
    journal.shutdown();
    journal.delete();
  }

  @Test
  public void test_appendAndGet() throws ExecutionException, InterruptedException {
    journal.start();
    List<Journal.AppendResult> results = new ArrayList<>();
    int count = 10000;
    CountDownLatch latch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      journal.append(
          new Journal.BytesData(("hello world " + i).getBytes(Utils.DEFAULT_CHARSET)),
          r -> {
            results.add(r);
            latch.countDown();
          });
    }
    latch.await();
    for (int i = 0; i < count; i++) {
      Journal.AppendResult result = results.get(i);
      Journal.BytesData record = journal.get(result.getOffset(), result.getSize()).get().getData();
      Assert.assertArrayEquals(
          ("hello world " + i).getBytes(Utils.DEFAULT_CHARSET), record.toByteArray());
    }

    for (int i = 0; i < count; i++) {
      Journal.AppendResult result = results.get(i);
      Journal.BytesData record = journal.get(result.getOffset()).get().getData();
      Assert.assertArrayEquals(
          ("hello world " + i).getBytes(Utils.DEFAULT_CHARSET), record.toByteArray());
    }
  }

  @Test
  public void test_groupAppend() {
    // TODO: manual trigger append
  }

  private void sendAndCheck() throws ExecutionException, InterruptedException {}
}
