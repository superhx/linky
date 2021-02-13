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

public class TimerWheel {
  /** SLOT[segmentIndex 4 bytes, offset 8 bytes] */
  private static final int SLOT_SIZE = 12;

  private static final byte[] NOOP_CURSOR = new byte[12];

  public TimerWheel() {
    ByteBuffer.wrap(NOOP_CURSOR).putInt(-1).putLong(-1);
  }

  public void putInflightSlot(long timeSecs, byte[] cursor) {}

  public byte[] getInflightSlot(long timeSecs) {
    return null;
  }
}
