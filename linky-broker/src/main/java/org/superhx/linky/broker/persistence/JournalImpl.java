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

import com.google.protobuf.InvalidProtocolBufferException;
import org.superhx.linky.service.proto.BatchRecord;

import java.nio.ByteBuffer;

public class JournalImpl extends AbstractJournal<BatchRecordJournalData> {
  public JournalImpl(String storePath) {
    super(storePath);
  }

  @Override
  protected Record<BatchRecordJournalData> parse(long offset, ByteBuffer byteBuffer) {
    ByteBuffer data = byteBuffer.slice();
    int size = data.getInt();
    int magicCode = data.getInt();
    if (magicCode == BLANK_MAGIC_CODE) {
      return Record.<BatchRecordJournalData>newBuilder()
          .setBlank(true)
          .setSize(size)
          .setOffset(offset)
          .build();
    }
    try {
      BatchRecord batchRecord = BatchRecord.parseFrom(data);
      return Record.<BatchRecordJournalData>newBuilder()
          .setSize(size)
          .setOffset(offset)
          .setData(new BatchRecordJournalData(batchRecord))
          .build();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    return null;
  }
}
