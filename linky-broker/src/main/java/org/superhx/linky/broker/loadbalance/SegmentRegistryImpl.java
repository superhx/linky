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
import org.superhx.linky.broker.Utils;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentRegistryImpl extends SegmentManagerServiceGrpc.SegmentManagerServiceImplBase
    implements SegmentRegistry {
  private Map<Long, Map<SegmentReplicaKey, Timestamped<SegmentMeta>>> segmentMap =
      new ConcurrentHashMap<>();

  private ControlNodeCnx controlNodeCnx;
  private NodeRegistry nodeRegistry;

  @Override
  public void register(SegmentMeta segment) {
    long topicPartitionId = Utils.topicPartitionId(segment.getTopicId(), segment.getPartition());
    Map<SegmentReplicaKey, Timestamped<SegmentMeta>> segments = segmentMap.get(topicPartitionId);
    if (segments == null) {
      segments = new ConcurrentHashMap<>();
      segmentMap.put(topicPartitionId, segments);
      segments = segmentMap.get(topicPartitionId);
    }
    for (SegmentMeta.Replica replica : segment.getReplicasList()) {
      SegmentReplicaKey key =
          new SegmentReplicaKey(
              segment.getTopicId(),
              segment.getPartition(),
              segment.getIndex(),
              replica.getAddress());
      segments.put(key, new Timestamped<>(segment));
    }
  }

  @Override
  public Map<Integer, SegmentMeta> getSegmentMetas(int topic, int partition) {
    long topicPartitionId = Utils.topicPartitionId(topic, partition);
    Map<SegmentReplicaKey, Timestamped<SegmentMeta>> segments = segmentMap.get(topicPartitionId);
    Map<Integer, SegmentMeta> indexReplicasMap = new HashMap<>();
    for (Timestamped<SegmentMeta> replica : segments.values()) {
      int index = replica.getData().getIndex();
      SegmentMeta segmentMeta = indexReplicasMap.get(index);
      if (segmentMeta == null) {
        segmentMeta = replica.getData();
        indexReplicasMap.put(index, segmentMeta);
        continue;
      }
      segmentMeta.getReplicasList().add(replica.getData().getReplicas(0));
    }
    return indexReplicasMap;
  }

  @Override
  public void create(
      SegmentManagerServiceProto.CreateRequest request,
      StreamObserver<SegmentManagerServiceProto.CreateResponse> responseObserver) {
    controlNodeCnx
        .createSegment(
            SegmentMeta.newBuilder()
                .setTopicId(request.getTopicId())
                .setPartition(request.getPartition())
                .setIndex(request.getLastIndex() + 1)
                .addReplicas(
                    SegmentMeta.Replica.newBuilder().setAddress(request.getAddress()).build())
                .build())
        .thenAccept(
            r -> {
              responseObserver.onNext(
                  SegmentManagerServiceProto.CreateResponse.newBuilder()
                      .setStatus(SegmentManagerServiceProto.CreateResponse.Status.SUCCESS)
                      .build());
            })
        .exceptionally(
            t -> {
              System.out.println("here");
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void getSegments(
      SegmentManagerServiceProto.GetSegmentsRequest request,
      StreamObserver<SegmentManagerServiceProto.GetSegmentsResponse> responseObserver) {
    long topicPartitionId = Utils.topicPartitionId(request.getTopicId(), request.getPartition());
    Map<SegmentReplicaKey, Timestamped<SegmentMeta>> segments = segmentMap.get(topicPartitionId);
    // TODO: fill segments
    responseObserver.onNext(SegmentManagerServiceProto.GetSegmentsResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  public void setControlNodeCnx(ControlNodeCnx controlNodeCnx) {
    this.controlNodeCnx = controlNodeCnx;
  }

  public void setNodeRegistry(NodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

}
