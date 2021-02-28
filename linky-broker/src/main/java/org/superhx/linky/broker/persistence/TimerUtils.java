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
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.BatchRecordOrBuilder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.superhx.linky.broker.persistence.Constants.*;

public class TimerUtils {
  public static Cursor getPreviousCursor(BatchRecord batchRecord) {
    return Optional.ofNullable(batchRecord.getRecords(0))
        .map(r -> r.getHeadersOrDefault(Constants.TIMER_PRE_CURSOR_HEADER, null))
        .map(b -> Cursor.get(b.toByteArray()))
        .orElse(Cursor.NOOP);
  }

  public static TimerIndex getTimerIndex(BatchRecordOrBuilder batchRecord) {
    TimerIndex timerIndex = new TimerIndex();
    if (isTimerCommit(batchRecord)) {
      ByteString timestampBytes =
          batchRecord.getRecords(0).getHeadersOrDefault(TIMER_TIMESTAMP_HEADER, null);
      long timestamp = Utils.getLong(timestampBytes.toByteArray());
      timerIndex.setSlot(slot(timestamp));
      byte[] timerIndexBytes = batchRecord.getRecords(0).getValue().toByteArray();
      timerIndex.setIndexes(timerIndexBytes);
    } else if (Flag.isTimer(batchRecord.getFlag())) {
      timerIndex.setSlot(slot(batchRecord.getVisibleTimestamp()));
    }
    return timerIndex;
  }

  public static TimerIndex transformTimerIndexRecord2TimerIndex(BatchRecordOrBuilder batchRecord) {
    ByteString slotBytes =
        batchRecord.getRecords(0).getHeadersOrDefault(TIMER_SLOT_RECORD_HEADER, null);
    int slot = Utils.getInt(slotBytes.toByteArray());
    TimerIndex timerIndex = new TimerIndex();
    timerIndex.setIndexes(batchRecord.getRecords(0).getValue().toByteArray());
    timerIndex.setSlot(slot);
    timerIndex.setCursor(batchRecord.getIndex(), batchRecord.getFirstOffset());
    timerIndex.setNext(new Cursor(batchRecord.getIndex(), batchRecord.getFirstOffset() + 1));
    return timerIndex;
  }

  public static byte[] getTimerIndexBytes(long timestamp, int index, long offset) {
    ByteBuffer indexesBuf = ByteBuffer.allocate(TIMER_INDEX_SIZE);
    indexesBuf.putLong(timestamp);
    indexesBuf.putInt(index);
    indexesBuf.putLong(offset);
    return indexesBuf.array();
  }

  public static byte[] toTimerIndexBytes(List<TimerIndex> timeIndexes) {
    if (timeIndexes.size() == 0) {
      return new byte[0];
    }
    int sum = timeIndexes.stream().mapToInt(t -> t.count()).sum();
    ByteBuffer buf = ByteBuffer.allocate(sum * Constants.TIMER_INDEX_SIZE);
    for (TimerIndex timerIndex : timeIndexes) {
      timerIndex.drainAsTimerIndex(buf);
    }
    return buf.array();
  }

  public static int slot(long timestamp) {
    if (timestamp == 0) {
      return -1;
    }
    return (int) ((timestamp / 1000) % TIMER_WINDOW);
  }

  public static boolean isTimerPrepare(BatchRecordOrBuilder batchRecord) {
    if (!((batchRecord.getFlag() & TIMER_FLAG) != 0
        && (batchRecord.getFlag() & INVISIBLE_FLAG) != 0)) {
      return false;
    }
    if (batchRecord.getRecordsCount() == 0) {
      return false;
    }
    ByteString bs = batchRecord.getRecords(0).getHeadersOrDefault(TIMER_TYPE_HEADER, null);
    return bs != null && Arrays.equals(TIMER_PREPARE_TYPE, bs.toByteArray());
  }

  public static boolean isTimerLink(BatchRecordOrBuilder batchRecord) {
    if ((batchRecord.getFlag() & TIMER_FLAG) == 0) {
      return false;
    }
    if ((batchRecord.getFlag() & LINK_FLAG) == 0) {
      return false;
    }
    return true;
  }

  public static boolean isTimerCommit(BatchRecordOrBuilder batchRecord) {
    if (!((batchRecord.getFlag() & TIMER_FLAG) != 0
        && (batchRecord.getFlag() & INVISIBLE_FLAG) != 0)) {
      return false;
    }
    if (batchRecord.getRecordsCount() == 0) {
      return false;
    }
    ByteString bs = batchRecord.getRecords(0).getHeadersOrDefault(TIMER_TYPE_HEADER, null);
    return bs != null && Arrays.equals(TIMER_COMMIT_TYPE, bs.toByteArray());
  }

  public static boolean isTimerIndex(BatchRecordOrBuilder batchRecord) {
    if (!((batchRecord.getFlag() & TIMER_FLAG) != 0
        && (batchRecord.getFlag() & INVISIBLE_FLAG) != 0)) {
      return false;
    }
    ByteString bs = batchRecord.getRecords(0).getHeadersOrDefault(TIMER_TYPE_HEADER, null);
    return bs != null && Arrays.equals(TIMER_INDEX_TYPE, bs.toByteArray());
  }

  public static int getSlot(BatchRecord batchRecord) {
    return Utils.getInt(
        batchRecord
            .getRecords(0)
            .getHeadersOrDefault(TIMER_SLOT_RECORD_HEADER, null)
            .toByteArray());
  }

  public static byte[] getTimerSlotSegmentKey(int slotSegment) {
    return ByteBuffer.allocate(TIMER_SLOT_SEGMENT_KEY_PREFIX.length + 4)
        .put(TIMER_SLOT_SEGMENT_KEY_PREFIX)
        .putInt(slotSegment)
        .array();
  }
}
