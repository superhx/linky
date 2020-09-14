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
package org.superhx.linky;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.KeepAlive;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyException;
import org.superhx.linky.broker.loadbalance.*;
import org.superhx.linky.broker.persistence.LocalSegmentManager;
import org.superhx.linky.broker.persistence.PartitionManager;
import org.superhx.linky.broker.persistence.PersistenceFactoryImpl;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.broker.service.PartitionService;
import org.superhx.linky.broker.service.RecordService;
import org.superhx.linky.broker.service.SegmentService;
import org.superhx.linky.service.proto.NodeMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LinkyBrokerStartup implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(LinkyBrokerStartup.class);
  private List<Lifecycle> components = new ArrayList<>();
  private Server server;
  private BrokerContext brokerContext;

  public LinkyBrokerStartup() {
    String port = System.getProperty("port", "9594");
    brokerContext = new BrokerContext();
    brokerContext.setAddress("127.0.0.1:" + port);
    brokerContext.setEpoch(System.currentTimeMillis());
    brokerContext.setNodeStatus(NodeMeta.Status.INIT);

    log.info("broker {} startup", port);

    LinkyElection election = new LinkyElection(brokerContext);
    components.add(election);

    DataNodeCnx dataNodeCnx = new DataNodeCnx();
    PersistenceFactoryImpl persistenceFactory = new PersistenceFactoryImpl();
    components.add(persistenceFactory);
    LocalSegmentManager localSegmentManager = new LocalSegmentManager();
    components.add(localSegmentManager);
    PartitionManager partitionManager = new PartitionManager();
    components.add(partitionManager);

    KVStore kvStore = new KVStore();
    components.add(kvStore);
    ControlNodeCnx controlNodeCnx = new ControlNodeCnx();
    components.add(controlNodeCnx);
    PartitionRegistryImpl partitionRegistry = new PartitionRegistryImpl();
    components.add(partitionRegistry);
    SegmentRegistryImpl segmentRegistry = new SegmentRegistryImpl();
    components.add(segmentRegistry);
    NodeRegistryImpl nodeRegistry = new NodeRegistryImpl();
    components.add(nodeRegistry);

    KeepAlive keepAlive = new KeepAlive();
    components.add(keepAlive);

    RecordService recordService = new RecordService();
    PartitionService partitionService = new PartitionService();
    SegmentService segmentService = new SegmentService();
    ControllerService controllerService = new ControllerService();

    brokerContext.setDataNodeCnx(dataNodeCnx);

    dataNodeCnx.setElection(election);
    dataNodeCnx.setBrokerContext(brokerContext);

    persistenceFactory.setBrokerContext(brokerContext);
    persistenceFactory.setLocalSegmentManager(localSegmentManager);

    localSegmentManager.setBrokerContext(brokerContext);
    localSegmentManager.setDataNodeCnx(dataNodeCnx);
    localSegmentManager.setPersistenceFactory(persistenceFactory);

    partitionManager.setPersistenceFactory(persistenceFactory);

    partitionRegistry.setNodeRegistry(nodeRegistry);
    partitionRegistry.setBrokerContext(brokerContext);
    partitionRegistry.setControlNodeCnx(controlNodeCnx);
    partitionRegistry.setElection(election);
    partitionRegistry.setKvStore(kvStore);

    segmentRegistry.setElection(election);
    segmentRegistry.setPartitionRegistry(partitionRegistry);
    segmentRegistry.setKvStore(kvStore);
    segmentRegistry.setBrokerContext(brokerContext);
    segmentRegistry.setControlNodeCnx(controlNodeCnx);
    segmentRegistry.setNodeRegistry(nodeRegistry);

    recordService.setPartitionManager(partitionManager);

    partitionService.setBrokerContext(brokerContext);
    partitionService.setPartitionManager(partitionManager);

    segmentService.setLocalSegmentManager(localSegmentManager);

    controllerService.setSegmentRegistry(segmentRegistry);
    controllerService.setNodeRegistry(nodeRegistry);

    keepAlive.setBrokerContext(brokerContext);
    keepAlive.setDataNodeCnx(dataNodeCnx);
    keepAlive.setLocalSegmentManager(localSegmentManager);

    election.registerListener(segmentRegistry);
    election.registerListener(partitionRegistry);

    partitionRegistry.createTopic("FOO", 1, 2);

    server =
        ServerBuilder.forPort(Integer.valueOf(port))
            .addService(recordService)
            .addService(partitionService)
            .addService(segmentService)
            .addService(controllerService)
            .addService(segmentRegistry)
            .build();
  }

  @Override
  public void init() {
    for (Lifecycle component : components) {
      component.init();
    }
  }

  @Override
  public void start() {
    try {
      server.start();
      for (Lifecycle component : components) {
        component.start();
      }
      brokerContext.setNodeStatus(NodeMeta.Status.ONLINE);
    } catch (Throwable e) {
      throw new LinkyException(e);
    }
  }

  @Override
  public void shutdown() {
    brokerContext.setNodeStatus(NodeMeta.Status.TAINT);
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    List<Lifecycle> components = new ArrayList<>(this.components);
    Collections.reverse(components);
    for (Lifecycle component : components) {
      component.shutdown();
    }
    brokerContext.setNodeStatus(NodeMeta.Status.OFFLINE);
  }

  public static void main(String... args) {
    LinkyBrokerStartup broker = new LinkyBrokerStartup();
    broker.init();
    broker.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> broker.shutdown()));
  }
}
