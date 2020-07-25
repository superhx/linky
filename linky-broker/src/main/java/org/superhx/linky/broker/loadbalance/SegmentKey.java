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
package org.superhx.linky.broker.loadbalance;

public class SegmentKey {
  private int topicId;
  private int partition;
  private int index;

  public SegmentKey(int topicId, int partition, int index) {
    this.topicId = topicId;
    this.partition = partition;
    this.index = index;
  }

  public int getTopicId() {
    return topicId;
  }

  public int getPartition() {
    return partition;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public boolean equals(Object obj) {
    return topicId == topicId && partition == partition && index == index;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + topicId;
    result = 31 * result + partition;
    result = 31 * result + index;
    return result;
  }

  @Override
  public String toString() {
    return "SegmentKey{topicId=" + topicId + ",partition=" + partition + ",index=" + index + "}";
  }
}
