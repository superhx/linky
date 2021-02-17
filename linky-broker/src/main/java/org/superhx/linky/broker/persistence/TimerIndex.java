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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static org.superhx.linky.broker.persistence.Constants.TIMER_INDEX_SIZE;

public class TimerIndex {
  private long timestamp;
  private int index;
  private long offset;

  public TimerIndex(long timestamp, int index, long offset) {
    this.timestamp = timestamp / 1000 * 1000;
    this.index = index;
    this.offset = offset;
  }

  public TimerIndex() {}

  public long getTimestamp() {
    return timestamp;
  }

  public TimerIndex setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public int getIndex() {
    return index;
  }

  public TimerIndex setIndex(int index) {
    this.index = index;
    return this;
  }

  public long getOffset() {
    return offset;
  }

  public TimerIndex setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  @Override
  public String toString() {
    return "TimerIndex{timestamp=" + timestamp + ",index=" + index + ",offset=" + offset + "}";
  }

  public static List<TimerIndex> getTimerIndexes(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    List<TimerIndex> timerIndexes = new LinkedList<>();
    for (int i = 0; i < bytes.length; i += 8 + 4 + 8) {
      timerIndexes.add(new TimerIndex(buf.getLong(), buf.getInt(), buf.getLong()));
    }
    return timerIndexes;
  }

  public static byte[] toBytes(List<TimerIndex> indexes) {
    byte[] indexesBytes = new byte[indexes.size() * TIMER_INDEX_SIZE];
    ByteBuffer indexesBuf = ByteBuffer.wrap(indexesBytes);
    for (TimerIndex timerIndex : indexes) {
      indexesBuf.putLong(timerIndex.getTimestamp());
      indexesBuf.putInt(timerIndex.getIndex());
      indexesBuf.putLong(timerIndex.getOffset());
    }
    return indexesBytes;
  }
}
