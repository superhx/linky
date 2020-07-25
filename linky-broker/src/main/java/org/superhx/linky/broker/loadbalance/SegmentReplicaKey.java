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

import java.util.Objects;

public class SegmentReplicaKey {
    private int topic;
    private int partition;
    private int index;
    private String address;

    public SegmentReplicaKey(int topic, int partition, int index, String address) {
        this.topic = topic;
        this.partition = partition;
        this.index = index;
        this.address = address;
    }

    public int getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public int getIndex() {
        return index;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SegmentReplicaKey)) {
            return false;
        }
        SegmentReplicaKey o = (SegmentReplicaKey) obj;
        return topic == o.topic && partition == o.partition && index == o.index && Objects.equals(address, o.address);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + topic;
        result = 31 * result + partition;
        result = 31 * result + index;
        result = 31 * result + Objects.hashCode(address);
        return result;
    }
}
