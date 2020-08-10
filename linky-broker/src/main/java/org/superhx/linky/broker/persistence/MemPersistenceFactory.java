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

import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.service.proto.PartitionMeta;
import org.superhx.linky.service.proto.SegmentMeta;

public class MemPersistenceFactory implements PersistenceFactory {
  private LocalSegmentManager localSegmentManager;
  private WriteAheadLog writeAheadLog;
  private BrokerContext brokerContext;

  @Override
  public Partition newPartition(PartitionMeta partitionMeta) {
    Partition partition = new LocalPartitionImpl(partitionMeta);
    ((LocalPartitionImpl) partition).setLocalSegmentManager(localSegmentManager);
    return partition;
  }

  @Override
  public Segment newSegment(SegmentMeta segmentMeta) {
    Segment segment = new LocalSegment(segmentMeta, newWriteAheadLog(), brokerContext);
    return segment;
  }

  @Override
  public synchronized WriteAheadLog newWriteAheadLog() {
    if (writeAheadLog != null) {
      return writeAheadLog;
    }
    writeAheadLog = new LocalWriteAheadLog(brokerContext.getStorePath() + "/wal/logs");
    return writeAheadLog;
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }
}
