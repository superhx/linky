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
import java.util.Objects;

import static org.superhx.linky.broker.persistence.Constants.NOOP_INDEX;
import static org.superhx.linky.broker.persistence.Constants.NOOP_OFFSET;

public class Cursor implements Comparable<Cursor> {
  public static final Cursor NOOP = new Cursor(NOOP_INDEX, NOOP_OFFSET);
  private int index;
  private long offset;

  public Cursor() {}

  public Cursor(int index, long offset) {
    this.index = index;
    this.offset = offset;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public Cursor next() {
    return new Cursor(index, offset + 1);
  }

  @Override
  public String toString() {
    return "Cursor{index=" + index + ",offset=" + offset + "}";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Cursor)) {
      return false;
    }
    Cursor o = (Cursor) obj;
    return o.index == index && o.offset == offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, offset);
  }

  public byte[] toBytes() {
    return ByteBuffer.allocate(12).putInt(index).putLong(offset).array();
  }

  static Cursor get(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    Cursor cursor = new Cursor();
    cursor.index = buf.getInt();
    cursor.offset = buf.getLong();
    return cursor;
  }

  @Override
  public int compareTo(Cursor o) {
    if (o == null) {
      return 1;
    }
    if (index != o.index) {
      return index - o.index;
    }
    return Long.compare(offset, o.offset);
  }
}
