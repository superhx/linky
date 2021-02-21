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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class Constants {
  public static final int INVISIBLE_FLAG = 1 << 0;
  public static final int META_FLAG = 1 << 1;
  public static final int TIMER_FLAG = 1 << 2;
  public static final int LINK_FLAG = 1 << 4;

  public static final int NOOP_INDEX = -1;
  public static final long NOOP_OFFSET = -1L;

  /** (4 bytes segment index, 8 bytes segment offset ) */
  public static final int TIMER_CURSOR_SIZE = 12;
  /** (timestamp, index, offset) */
  public static final int TIMER_INDEX_SIZE = 8 + 4 + 8;
  /** (index, offset) */
  public static final int TIMER_LINK_SIZE = 4 + 8;

  public static final int TIMER_WINDOW = (int) TimeUnit.DAYS.toSeconds(1);
  public static final int TIMER_WHEEL_SEGMENT = (int) TimeUnit.MINUTES.toSeconds(5);

  public static final long TIMESTAMP_BARRIER_SAFE_WINDOW = TimeUnit.MILLISECONDS.toMillis(100);

  public static final String TIMER_TIMESTAMP_HEADER = Utils.base16(Utils.getBytes(1));
  public static final String TIMER_PREPARE_CURSOR_HEADER = Utils.base16(Utils.getBytes(2));
  public static final String TIMER_SLOT_RECORD_HEADER = Utils.base16(Utils.getBytes(3));
  public static final String TIMER_PRE_CURSOR_HEADER = Utils.base16(Utils.getBytes(4));
  /** 0: timer index, 1: timer trigger, 2 timer commit */
  public static final String TIMER_TYPE_HEADER = Utils.base16(Utils.getBytes(5));

  public static final byte[] TIMER_INDEX_TYPE = Utils.getBytes(0);
  public static final byte[] TIMER_PREPARE_TYPE = Utils.getBytes(1);
  public static final byte[] TIMER_COMMIT_TYPE = Utils.getBytes(2);

  public static final byte[] NOOP_CURSOR_SEGMENT = new byte[1];
  public static final byte[] NOOP_CURSOR = ByteBuffer.allocate(12).putInt(-1).putLong(-1L).array();

  /** meta key start */
  public static final byte[] TIMER_SLOT_SEGMENT_KEY_PREFIX = Utils.getBytes(1);

  public static final byte[] TIMER_INDEX_BUILD_LSO_KEY = Utils.getBytes(2);

  public static final byte[] TIMER_NEXT_TIMESTAMP_KEY = Utils.getBytes(3);

  /** meta key end */
}
