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

public class Flag {

  public static boolean isVisible(int flag) {
    return (flag & Constants.INVISIBLE_FLAG) == 0;
  }

  public static boolean isTimer(int flag) {
    return (flag & Constants.TIMER_FLAG) != 0;
  }

  public static boolean isTransMsg(int flag) {
    return (flag & Constants.TRANS_MSG_FLAG) != 0;
  }

  public static boolean isTransConfirm(int flag) {
    return (flag & Constants.TRANS_CONFIRM_FLAG) != 0;
  }
}
