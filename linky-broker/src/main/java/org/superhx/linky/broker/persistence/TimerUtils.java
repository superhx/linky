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

import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.superhx.linky.broker.persistence.Constants.TIMER_SLOT_RECORD_HEADER;
import static org.superhx.linky.broker.persistence.Constants.TIMER_SLOT_SEGMENT_KEY_PREFIX;

public class TimerUtils {
  public static Cursor getPreviousCursor(BatchRecord batchRecord) {
    return Optional.ofNullable(batchRecord.getRecords(0))
        .map(r -> r.getHeadersOrDefault(Constants.TIMER_PRE_CURSOR_HEADER, null))
        .map(b -> Cursor.get(b.toByteArray()))
        .orElse(Cursor.NOOP);
  }

  public static List<TimerIndex> getTimerIndexes(BatchRecord batchRecord) {
    List<TimerIndex> timerIndexes = new LinkedList<>();
    timerIndexes.add(
        new TimerIndex()
            .setIndex(batchRecord.getIndex())
            .setOffset(batchRecord.getFirstOffset())
            .setTimestamp(batchRecord.getVisibleTimestamp())
            .setNext(
                new Cursor(
                    batchRecord.getIndex(),
                    batchRecord.getFirstOffset() + Utils.getOffsetCount(batchRecord))));
    return timerIndexes;
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
