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

import io.grpc.stub.StreamObserver;
import org.superhx.linky.service.proto.ControllerServiceGrpc;
import org.superhx.linky.service.proto.ControllerServiceProto;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.SegmentMeta;

public class ControllerService extends ControllerServiceGrpc.ControllerServiceImplBase {
  private NodeRegistry nodeRegistry;
  private SegmentRegistry segmentRegistry;

  @Override
  public void heartbeat(
      ControllerServiceProto.HeartbeatRequest request,
      StreamObserver<ControllerServiceProto.HeartbeatResponse> responseObserver) {
    nodeRegistry
        .register(NodeMeta.newBuilder().setAddress(request.getAddress()).setEpoch(request.getEpoch()).build())
        .thenAccept(
            r -> {
              responseObserver.onNext(
                  ControllerServiceProto.HeartbeatResponse.newBuilder().build());
              responseObserver.onCompleted();
            });
    for (SegmentMeta segmentMeta : request.getSegmentsList()) {
      segmentRegistry.register(segmentMeta);
    }
  }

  public void setNodeRegistry(NodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

  public void setSegmentRegistry(SegmentRegistry segmentRegistry) {
    this.segmentRegistry = segmentRegistry;
  }
}
