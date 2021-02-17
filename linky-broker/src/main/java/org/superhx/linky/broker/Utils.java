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

import com.google.common.io.BaseEncoding;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.PartitionMeta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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

  public static void byte2file(byte[] bytes, String fileName) {
    try {
      ensureDirOK(new File(fileName).getParent());
      Files.write(bytes, new File(fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static byte[] pb2jsonBytes(MessageOrBuilder builder) {
    try {
      return JsonFormat.printer().print(builder).getBytes(DEFAULT_CHARSET);
    } catch (InvalidProtocolBufferException e) {
      throw new LinkyIOException(e);
    }
  }

  public static void jsonBytes2pb(byte[] json, com.google.protobuf.Message.Builder builder) {
    try {
      JsonFormat.parser().merge(new String(json, DEFAULT_CHARSET), builder);
    } catch (InvalidProtocolBufferException e) {
      throw new LinkyIOException(e);
    }
  }

  public static void jsonObj2file(Object json, String file) {
    String str = new Gson().toJson(json);
    str2file(str, file);
  }

  public static <T> T file2jsonObj(String path, Class<T> clazz) {
    File file = new File(path);
    if (!file.exists()) {
      return null;
    }
    try {
      String str = Files.asCharSource(file, DEFAULT_CHARSET).read();
      return new Gson().fromJson(str, clazz);
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  public static NodeMeta partitionMeta2NodeMeta(PartitionMeta meta) {
    if (meta == null) {
      return null;
    }
    return NodeMeta.newBuilder()
        .setAddress(meta.getAddress())
        .setEpoch(meta.getEpoch())
        .setStatus(NodeMeta.Status.ONLINE)
        .build();
  }

  public static ExecutorService newFixedThreadPool(int size, String name) {
    return Executors.newFixedThreadPool(size, r -> new Thread(r, name));
  }

  public static ScheduledExecutorService newScheduledThreadPool(int size, String name) {
    return Executors.newScheduledThreadPool(size, r -> new Thread(r, name));
  }

  public static byte[] getBytes(int num) {
    return ByteBuffer.allocate(4).putInt(num).array();
  }

  public static byte[] getBytes(long num) {
    return ByteBuffer.allocate(8).putLong(num).array();
  }

  public static String base16(byte[] bytes) {
    return BaseEncoding.base16().encode(bytes);
  }
}
