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
package org.superhx.linky.broker;

import org.superhx.linky.broker.persistence.LocalSegmentManager;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.service.proto.ControllerServiceProto;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KeepAlive implements Lifecycle {
  private ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();

  private BrokerContext brokerContext;
  private DataNodeCnx dataNodeCnx;
  private LocalSegmentManager localSegmentManager;

  @Override
  public void start() {
    schedule.scheduleWithFixedDelay(() -> keepAlive(), 0, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    schedule.shutdown();
  }

  public void keepAlive() {
    try {
      ControllerServiceProto.HeartbeatRequest heartbeatRequest =
              ControllerServiceProto.HeartbeatRequest.newBuilder()
                      .setNode(brokerContext.getNodeMeta())
                      .addAllSegments(localSegmentManager.getLocalSegments())
                      .build();
      dataNodeCnx.keepalive(heartbeatRequest);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  public void setDataNodeCnx(DataNodeCnx dataNodeCnx) {
    this.dataNodeCnx = dataNodeCnx;
  }

  public void setLocalSegmentManager(LocalSegmentManager localSegmentManager) {
    this.localSegmentManager = localSegmentManager;
  }
}
