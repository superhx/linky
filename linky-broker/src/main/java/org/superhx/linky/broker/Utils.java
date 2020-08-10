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
package org.superhx.linky.broker;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.NumberFormat;

public class Utils {
  public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
  public static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  public static long topicPartitionId(int topic, int partition) {
    return (((long) topic) << 32) | (long) partition;
  }

  public static void ensureDirOK(final String dirName) {
    if (dirName != null) {
      File f = new File(dirName);
      if (!f.exists()) {
        f.mkdirs();
      }
    }
  }

  public static String offset2FileName(final long offset) {
    final NumberFormat nf = NumberFormat.getInstance();
    nf.setMinimumIntegerDigits(20);
    nf.setMaximumFractionDigits(0);
    nf.setGroupingUsed(false);
    return nf.format(offset);
  }

  public static final String getSegmentMetaPath(
      String basePath, int topicId, int partitionId, int index) {
    return String.format("%s/segments/%s/%s/%s/meta.json", basePath, topicId, partitionId, index);
  }

  public static void str2file(String str, String fileName) {
    try {
      ensureDirOK(new File(fileName).getParent());
      Files.write(str.getBytes(DEFAULT_CHARSET), new File(fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  public static String getSegmentHex(int topicId, int partition, int index) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(12);
    byteBuffer.putInt(topicId);
    byteBuffer.putInt(partition);
    byteBuffer.putInt(index);
    byteBuffer.flip();
    return bytesToHex(byteBuffer.array());
  }
}
