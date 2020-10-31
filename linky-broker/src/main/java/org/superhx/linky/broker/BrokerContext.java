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

import org.superhx.linky.broker.persistence.ChunkManager;
import org.superhx.linky.broker.persistence.PersistenceFactory;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.service.proto.NodeMeta;

public class BrokerContext {
  private String storePath;
  private DataNodeCnx dataNodeCnx;
  private NodeMeta.Builder nodeMeta = NodeMeta.newBuilder();
  private PersistenceFactory persistenceFactory;
  private ChunkManager chunkManager;

  public String getAddress() {
    return nodeMeta.getAddress();
  }

  public void setAddress(String address) {
    nodeMeta.setAddress(address);
  }

  public long getEpoch() {
    return nodeMeta.getEpoch();
  }

  public void setEpoch(long epoch) {
    nodeMeta.setEpoch(epoch);
  }

  public NodeMeta.Status getNodeStatus() {
    return nodeMeta.getStatus();
  }

  public void setNodeStatus(NodeMeta.Status nodeStatus) {
    nodeMeta.setStatus(nodeStatus);
  }

  public String getStorePath() {
    return System.getProperty("user.home") + "/linky/" + getAddress();
  }

  public void setStorePath(String storePath) {
    this.storePath = storePath;
  }

  public DataNodeCnx getDataNodeCnx() {
    return dataNodeCnx;
  }

  public void setDataNodeCnx(DataNodeCnx dataNodeCnx) {
    this.dataNodeCnx = dataNodeCnx;
  }

  public NodeMeta getNodeMeta() {
    return nodeMeta.build();
  }

  public void setNodeMeta(NodeMeta nodeMeta) {
    this.nodeMeta = nodeMeta.toBuilder();
  }

  public PersistenceFactory getPersistenceFactory() {
    return persistenceFactory;
  }

  public void setPersistenceFactory(PersistenceFactory persistenceFactory) {
    this.persistenceFactory = persistenceFactory;
  }

  public ChunkManager getChunkManager() {
    return chunkManager;
  }

  public void setChunkManager(ChunkManager chunkManager) {
    this.chunkManager = chunkManager;
  }
}
