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

import org.rocksdb.*;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.ChunkMeta;
import org.superhx.linky.service.proto.SegmentMeta;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistentMeta {
  static {
    RocksDB.loadLibrary();
  }

  private static final byte[] CHUNK_META_COLUMN_FAMILY = "CM".getBytes(Utils.DEFAULT_CHARSET);
  private static final byte[] SEGMENT_META_COLUMN_FAMILY = "SM".getBytes(Utils.DEFAULT_CHARSET);

  private String metaPath;
  private RocksDB rocksDB;
  private ColumnFamilyHandle chunkMetaCFH;
  private ColumnFamilyHandle segmentMetaCFH;

  public PersistentMeta(String path) {
    metaPath = path + "/meta";
    Utils.ensureDirOK(metaPath);
    DBOptions options =
        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        Arrays.asList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
            new ColumnFamilyDescriptor(CHUNK_META_COLUMN_FAMILY, cfOptions),
            new ColumnFamilyDescriptor(SEGMENT_META_COLUMN_FAMILY, cfOptions));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try {
      rocksDB = RocksDB.open(options, metaPath, cfDescriptors, columnFamilyHandleList);
      chunkMetaCFH = columnFamilyHandleList.get(1);
      segmentMetaCFH = columnFamilyHandleList.get(2);
    } catch (RocksDBException e) {
      throw new LinkyIOException(e);
    }
  }

  public RocksIterator chunkMetaIterator() {
    return rocksDB.newIterator(chunkMetaCFH, new ReadOptions());
  }

  public void putChunkMeta(ChunkMeta chunkMeta) {
    ByteBuffer key = ByteBuffer.allocate(4);
    key.putInt(chunkMeta.getChunkId());
    byte[] value = chunkMeta.toByteArray();
    try {
      rocksDB.put(chunkMetaCFH, key.array(), value);
    } catch (RocksDBException e) {
      throw new LinkyIOException(e);
    }
  }

  public RocksIterator segmentMetaIterator() {
    return rocksDB.newIterator(segmentMetaCFH, new ReadOptions());
  }

  public void putSegmentMeta(SegmentMeta segmentMeta) {
    ByteBuffer key = ByteBuffer.allocate(16);
    key.putInt(segmentMeta.getTopicId());
    key.putInt(segmentMeta.getPartition());
    key.putInt(segmentMeta.getIndex());
    byte[] value = segmentMeta.toByteArray();
    try {
      rocksDB.put(segmentMetaCFH, new WriteOptions().setSync(true), key.array(), value);
    } catch (RocksDBException e) {
      throw new LinkyIOException(e);
    }
  }
}
