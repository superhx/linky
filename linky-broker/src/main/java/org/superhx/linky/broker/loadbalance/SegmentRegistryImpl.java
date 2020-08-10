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

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Segment.NO_OFFSET;

public class SegmentRegistryImpl extends SegmentManagerServiceGrpc.SegmentManagerServiceImplBase
    implements SegmentRegistry {
  private static final Logger log = LoggerFactory.getLogger(SegmentRegistryImpl.class);
  private Map<SegmentKey, SegmentMeta.Builder> segments = new ConcurrentHashMap<>();
  private Map<Long, Map<SegmentReplicaKey, Timestamped<SegmentMeta>>> aliveSegments =
      new ConcurrentHashMap<>();

  private ControlNodeCnx controlNodeCnx;
  private NodeRegistry nodeRegistry;
  private BrokerContext brokerContext;
  private KVStore kvStore;

  public void init() {
    try {
      kvStore
          .get("segments/")
          .thenAccept(
              r -> {
                r.getKvs().stream()
                    .forEach(
                        m -> {
                          try {
                            SegmentMeta meta = SegmentMeta.parseFrom(m.getValue().getBytes());
                            segments.put(
                                new SegmentKey(
                                    meta.getTopicId(), meta.getPartition(), meta.getIndex()),
                                meta.toBuilder());
                            log.info("load segment config {}", meta);
                          } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                          }
                        });
              })
          .get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void register(SegmentMeta segment) {
    long topicPartitionId = Utils.topicPartitionId(segment.getTopicId(), segment.getPartition());
    Map<SegmentReplicaKey, Timestamped<SegmentMeta>> segments = aliveSegments.get(topicPartitionId);
    if (segments == null) {
      segments = new ConcurrentHashMap<>();
      aliveSegments.put(topicPartitionId, segments);
      segments = aliveSegments.get(topicPartitionId);
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
  public void create(
      SegmentManagerServiceProto.CreateRequest request,
      StreamObserver<SegmentManagerServiceProto.CreateResponse> responseObserver) {
    int replicaNum = 1;
    List<String> replicas = new ArrayList<>(replicaNum);
    replicas.add(request.getAddress());
    for (NodeMeta node : nodeRegistry.getAliveNodes()) {
      if (node.getAddress().equals(request.getAddress())) {
        continue;
      }
      if (replicas.size() < replicaNum) {
        replicas.add(node.getAddress());
      }
    }
    SegmentMeta.Builder builder =
        SegmentMeta.newBuilder()
            .setTopicId(request.getTopicId())
            .setPartition(request.getPartition())
            .setStartOffset(request.getStartOffset())
            .setIndex(request.getLastIndex() + 1);
    for (String address : replicas) {
      int flag = address.equals(request.getAddress()) ? 0 : 1;
      builder.addReplicas(
          SegmentMeta.Replica.newBuilder().setAddress(address).setFlag(flag).build());
    }

    SegmentMeta meta = builder.build();
    controlNodeCnx
        .createSegment(meta)
        .thenAccept(
            r -> {
              kvStore.put(
                  "segments/"
                      + Utils.getSegmentHex(
                          request.getTopicId(), request.getPartition(), request.getLastIndex() + 1),
                  builder.clone().build().toByteArray());
              this.segments.put(
                  new SegmentKey(
                      request.getTopicId(), request.getPartition(), request.getLastIndex() + 1),
                  builder);
              responseObserver.onNext(
                  SegmentManagerServiceProto.CreateResponse.newBuilder()
                      .setStatus(SegmentManagerServiceProto.CreateResponse.Status.SUCCESS)
                      .build());
              responseObserver.onCompleted();
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
    List<SegmentMeta> metas = new ArrayList<>();
    this.segments.forEach(
        (k, v) -> {
          if (request.getTopicId() == k.getTopicId()
              && request.getPartition() == k.getPartition()) {
            metas.add(v.build());
          }
        });
    Collections.sort(metas, Comparator.comparing(SegmentMeta::getIndex));
    responseObserver.onNext(
        SegmentManagerServiceProto.GetSegmentsResponse.newBuilder().addAllSegments(metas).build());
    responseObserver.onCompleted();
  }

  @Override
  public void seal(
      SegmentManagerServiceProto.SealRequest request,
      StreamObserver<SegmentManagerServiceProto.SealResponse> responseObserver) {
    SegmentMeta.Builder meta =
        this.segments.get(
            new SegmentKey(request.getTopicId(), request.getPartition(), request.getIndex()));
    List<Long> endOffsets = new LinkedList<>();
    CompletableFuture.allOf(
            meta.getReplicasList().stream()
                .map(
                    r -> {
                      CompletableFuture<Void> rst = new CompletableFuture<>();
                      controlNodeCnx
                          .getSegmentServiceStub(r.getAddress())
                          .seal(
                              SegmentServiceProto.SealRequest.newBuilder()
                                  .setTopicId(request.getTopicId())
                                  .setPartition(request.getPartition())
                                  .setIndex(request.getIndex())
                                  .build(),
                              new StreamObserver<SegmentServiceProto.SealResponse>() {
                                @Override
                                public void onNext(SegmentServiceProto.SealResponse sealResponse) {
                                  endOffsets.add(sealResponse.getEndOffset());
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                  endOffsets.add(NO_OFFSET);
                                }

                                @Override
                                public void onCompleted() {
                                  rst.complete(null);
                                }
                              });
                      return rst;
                    })
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[0]))
        .thenAccept(
            r -> {
              Collections.sort(endOffsets);
              long endOffset = endOffsets.get(endOffsets.size() / 2) + 1;
              meta.setEndOffset(endOffset);
              kvStore.put(
                  "segments/"
                      + Utils.getSegmentHex(
                          request.getTopicId(), request.getPartition(), request.getIndex()),
                  meta.clone().build().toByteArray());
              responseObserver.onNext(
                  SegmentManagerServiceProto.SealResponse.newBuilder()
                      .setEndOffset(endOffset)
                      .build());

              responseObserver.onCompleted();
            })
        .exceptionally(
            t -> {
              responseObserver.onError(t);
              return null;
            });
  }

  public void setControlNodeCnx(ControlNodeCnx controlNodeCnx) {
    this.controlNodeCnx = controlNodeCnx;
  }

  public void setNodeRegistry(NodeRegistry nodeRegistry) {
    this.nodeRegistry = nodeRegistry;
  }

  public void setBrokerContext(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
  }

  public void setKvStore(KVStore kvStore) {
    this.kvStore = kvStore;
  }
}
