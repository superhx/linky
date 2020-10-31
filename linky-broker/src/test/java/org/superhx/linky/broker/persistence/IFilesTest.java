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
import org.superhx.linky.broker.Utils;

import java.nio.ByteBuffer;

public abstract class IFilesTest {
  public static final String path = System.getProperty("user.home") + "/linkytest/ifilestest";

  protected IFiles ifiles;

  @Before
  public void setup() {
    ifiles = new ChannelFiles(path,"ifilestest", 1024, (m, s) -> 0L, (s) -> ByteBuffer.allocate(s));
  }

  @After
  public void destroy() {
    ifiles.delete();
  }

  @Test
  public void test_appendAndRead() throws Exception {
    testWrite(1024);
    testRead(1024);
  }

  public void testWrite(int count) {
    for (int i = 0; i < count; i++) {
      String str = "hellowo" + (i % 10);
      ByteBuffer buf = ByteBuffer.wrap(str.getBytes(Utils.DEFAULT_CHARSET));
      ifiles.append(buf);
    }
  }

  public void testRead(int count) throws Exception {
    int pos = 0;
    for (int i = 0; i < count; i++) {
      byte[] expect = ("hellowo" + (i % 10)).getBytes(Utils.DEFAULT_CHARSET);
      ByteBuffer buf = ifiles.read(pos, expect.length).get();
      Assert.assertEquals(expect.length, buf.remaining());
      byte[] data = new byte[expect.length];
      buf.get(data);
      Assert.assertArrayEquals(expect, data);
      pos += expect.length;
    }
  }
}
