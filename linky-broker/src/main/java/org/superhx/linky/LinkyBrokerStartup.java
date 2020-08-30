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
import org.superhx.linky.broker.loadbalance.*;
import org.superhx.linky.broker.persistence.LocalSegmentManager;
import org.superhx.linky.broker.persistence.MemPersistenceFactory;
import org.superhx.linky.broker.persistence.PersistenceFactory;
import org.superhx.linky.broker.service.DataNodeCnx;
import org.superhx.linky.broker.service.PartitionService;
import org.superhx.linky.broker.service.RecordService;
import org.superhx.linky.broker.service.SegmentService;
import org.superhx.linky.service.proto.ControllerServiceProto;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LinkyBrokerStartup {
  private static final Logger log = LoggerFactory.getLogger(LinkyBrokerStartup.class);
  private ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();
  Server server;

  public LinkyBrokerStartup() {
    long epoch = System.currentTimeMillis();

    String port = System.getProperty("port", "9594");
    BrokerContext brokerContext = new BrokerContext();
    brokerContext.setAddress("127.0.0.1:" + port);
    log.info("broker {} startup", port);

    KVStore kvStore = new KVStore();

    RecordService recordService = new RecordService();
    PartitionService partitionService = new PartitionService();
    SegmentService segmentService = new SegmentService();
    DataNodeCnx dataNodeCnx = new DataNodeCnx();
    LocalSegmentManager localSegmentManager = new LocalSegmentManager();
    PersistenceFactory persistenceFactory = new MemPersistenceFactory();

    recordService.setPartitionService(partitionService);
    partitionService.setPersistenceFactory(persistenceFactory);
    segmentService.setLocalSegmentManager(localSegmentManager);
    localSegmentManager.setDataNodeCnx(dataNodeCnx);
    localSegmentManager.setPersistenceFactory(persistenceFactory);
    localSegmentManager.setBrokerContext(brokerContext);
    ((MemPersistenceFactory) persistenceFactory).setBrokerContext(brokerContext);
    ((MemPersistenceFactory) persistenceFactory).setLocalSegmentManager(localSegmentManager);
    dataNodeCnx.setBrokerContext(brokerContext);
    brokerContext.setDataNodeCnx(dataNodeCnx);

    localSegmentManager.init();

    ControlNodeCnx controlNodeCnx = new ControlNodeCnx();
    PartitionRegistry partitionRegistry = new PartitionRegistryImpl();
    NodeRegistry nodeRegistry = new NodeRegistryImpl();
    SegmentRegistryImpl segmentRegistry = new SegmentRegistryImpl();
    ControllerService controllerService = new ControllerService();

    LinkyElection election = new LinkyElection(brokerContext);
    ((PartitionRegistryImpl) partitionRegistry).setControlNodeCnx(controlNodeCnx);
    ((PartitionRegistryImpl) partitionRegistry).setNodeRegistry(nodeRegistry);
    ((PartitionRegistryImpl) partitionRegistry).setSegmentRegistry(segmentRegistry);
    ((PartitionRegistryImpl) partitionRegistry).setKvStore(kvStore);
    segmentRegistry.setControlNodeCnx(controlNodeCnx);
    segmentRegistry.setNodeRegistry(nodeRegistry);
    segmentRegistry.setBrokerContext(brokerContext);
    segmentRegistry.setKvStore(kvStore);
    segmentRegistry.setPartitionRegistry(partitionRegistry);
    segmentRegistry.setElection(election);
    election.registerListener(segmentRegistry);
    election.registerListener((PartitionRegistryImpl) partitionRegistry);
    controllerService.setNodeRegistry(nodeRegistry);
    controllerService.setSegmentRegistry(segmentRegistry);
    ((PartitionRegistryImpl) partitionRegistry).setElection(election);
    dataNodeCnx.setElection(election);

    segmentRegistry.init();

    server =
        ServerBuilder.forPort(Integer.valueOf(port))
            .addService(recordService)
            .addService(partitionService)
            .addService(segmentService)
            .addService(controllerService)
            .addService(segmentRegistry)
            .build();

    partitionRegistry.start();
    partitionRegistry.createTopic("FOO", 1, 3);

    schedule.scheduleWithFixedDelay(
        () -> {
          localSegmentManager.getLocalSegments();
          ControllerServiceProto.HeartbeatRequest heartbeatRequest =
              ControllerServiceProto.HeartbeatRequest.newBuilder()
                  .setAddress(brokerContext.getAddress())
                  .setEpoch(epoch)
                  .addAllSegments(localSegmentManager.getLocalSegments())
                  .build();
          dataNodeCnx.keepalive(heartbeatRequest);
        },
        1000,
        1000,
        TimeUnit.MILLISECONDS);
  }

  public void start() throws IOException, InterruptedException {
    server.start();
    server.awaitTermination();
  }

  public static void main(String... args) throws IOException, InterruptedException {

    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
    new LinkyBrokerStartup().start();
  }
}
