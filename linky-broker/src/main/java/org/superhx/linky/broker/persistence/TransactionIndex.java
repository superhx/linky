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

public class TransactionIndex {
  private Cursor cursor;

  public Cursor getCursor() {
    return cursor;
  }

  public void setCursor(Cursor cursor) {
    this.cursor = cursor;
  }

  public static Trans trans(Cursor cursor) {
    Trans trans = new Trans();
    trans.setCursor(cursor);
    return trans;
  }

  public static Normal normal(Cursor cursor) {
    Normal normal = new Normal();
    normal.setCursor(cursor);
    return normal;
  }

  public static Confirm confirm(ByteBuffer buf) {
    return TransactionIndex.Confirm.getConfirm(buf);
  }

  static class Normal extends TransactionIndex {}

  static class Trans extends TransactionIndex {}

  static class Confirm extends TransactionIndex {
    private static final int CURSOR_SIZE = 12;
    private boolean commit;
    private List<Cursor> confirmCursors;

    public boolean isCommit() {
      return commit;
    }

    public void setCommit(boolean commit) {
      this.commit = commit;
    }

    public List<Cursor> getConfirmCursors() {
      return confirmCursors;
    }

    public void setConfirmCursors(List<Cursor> confirmCursors) {
      this.confirmCursors = confirmCursors;
    }

    public static Confirm getConfirm(ByteBuffer buf) {
      Confirm confirm = new Confirm();
      confirm.setCommit(buf.get() == 0);
      List<Cursor> offsets = new LinkedList<>();
      for (; buf.remaining() >= CURSOR_SIZE; ) {
        Cursor offset = new Cursor(buf.getInt(), buf.getLong());
        offsets.add(offset);
      }
      confirm.setConfirmCursors(offsets);
      return confirm;
    }
  }
}
