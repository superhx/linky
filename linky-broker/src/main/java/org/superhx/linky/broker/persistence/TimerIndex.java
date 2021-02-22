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
import java.util.List;

import static org.superhx.linky.broker.persistence.Constants.TIMER_INDEX_SIZE;

public class TimerIndex {
  private Cursor cursor;
  private Cursor next;
  private int slot = -1;
  private byte[] indexes;

  public boolean isTimer() {
    return slot >= 0;
  }

  public void setSlot(int slot) {
    this.slot = slot;
  }

  public int getSlot() {
    return slot;
  }

  public Cursor getNext() {
    return next;
  }

  public void setNext(Cursor next) {
    this.next = next;
  }

  public int count() {
    return indexes.length / TIMER_INDEX_SIZE;
  }

  public void setIndexes(byte[] indexes) {
    this.indexes = indexes;
  }

  public TimerIndex setCursor(int index, long offset) {
    cursor = new Cursor(index, offset);
    return this;
  }

  public TimerIndex setTimerIndex(long timestamp, int index, long offset) {
    if (isTimer()) {
      indexes = TimerUtils.getTimerIndexBytes(timestamp, index, offset);
    }
    return this;
  }

  public Cursor getCursor() {
    return cursor;
  }

  public void drainAsTimerIndex(ByteBuffer buf) {
    if (indexes == null) {
      return;
    }
    buf.put(indexes);
  }

  public void split(long timestamp, List<TimerIndex> matched, List<TimerIndex> unmatched) {
    if (indexes == null) {
      return;
    }
    ByteBuffer buf = ByteBuffer.wrap(indexes);
    for (int i = 0; i < count(); i++) {
      long tim = buf.getLong() / 1000 * 1000;
      int index = buf.getInt();
      long offset = buf.getLong();
      TimerIndex timerIndex = new TimerIndex();
      timerIndex.setSlot(slot);
      timerIndex.setTimerIndex(tim, index, offset);
      if (tim <= timestamp) {
        matched.add(timerIndex);
      } else {
        unmatched.add(timerIndex);
      }
    }
  }

  public byte[] getTimerIndexBytes() {
    return indexes;
  }
}
