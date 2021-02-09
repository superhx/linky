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

import com.google.protobuf.TextFormat;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.persistence.Segment;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceGrpc;
import org.superhx.linky.controller.service.proto.SegmentManagerServiceProto;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.NodeMeta;
import org.superhx.linky.service.proto.SegmentMeta;
import org.superhx.linky.service.proto.TopicMeta;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Segment.*;

public class SegmentRegistryImpl extends SegmentManagerServiceGrpc.SegmentManagerServiceImplBase
    implements SegmentRegistry, Lifecycle, LinkyElection.LeaderChangeListener {
  private static final Logger log = LoggerFactory.getLogger(SegmentRegistryImpl.class);
  private static final long KEEPALIVE = TimeUnit.SECONDS.toMillis(10);
  private Map<SegmentKey, SegmentMeta.Builder> segments = null;
  private Map<SegmentKey, Map<String, Timestamped<SegmentMeta>>> aliveSegments =
      new ConcurrentHashMap<>();

  private ControlNodeCnx controlNodeCnx;
  private NodeRegistry nodeRegistry;
  private BrokerContext brokerContext;
  private PartitionRegistry partitionRegistry;
  private KVStore kvStore;
  private LinkyElection election;

  private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  public void init() {
    scheduler.scheduleWithFixedDelay(
        () -> {
          try {
            checkSegments();
          } catch (Throwable e) {
            e.printStackTrace();
          }
        },
        30,
        30,
        TimeUnit.SECONDS);
  }

  private void checkSegments() {
    if (!election.isLeader()) {
      return;
    }

    List<NodeMeta> nodeMetas = nodeRegistry.getAliveNodes();
    Map<SegmentKey, SegmentMeta.Builder> segments = this.segments;
    for (Map.Entry<SegmentKey, SegmentMeta.Builder> entry : segments.entrySet()) {
      SegmentMeta.Builder metaBuilder = entry.getValue();
      if ((metaBuilder.getFlag() & SEAL_MARK) == 0) {
        continue;
      }
      int topicId = entry.getKey().getTopicId();

      // ensure replica num
      Map<String, SegmentMeta> onlineSegment =
          getOnlineReplicaMetas(metaBuilder, TimeUnit.MINUTES.toMillis(1));
      Set<String> replicas = new HashSet<>(onlineSegment.keySet());
      for (int i = replicas.size();
          i < partitionRegistry.getTopicMeta(topicId).getReplicaNum();
          i++) {
        for (NodeMeta nodeMeta : nodeMetas) {
          if (replicas.contains(nodeMeta.getAddress())) {
            continue;
          }

          SegmentMeta.Builder replicaSegment = entry.getValue().clone().clearReplicas();

          replicaSegment.addReplicas(
              SegmentMeta.Replica.newBuilder()
                  .setAddress(nodeMeta.getAddress())
                  .setFlag(FOLLOWER_MARK)
                  .build());
          replicas.add(nodeMeta.getAddress());
          log.info("[REPLICA_LOSS]{}", TextFormat.shortDebugString(replicaSegment));
          controlNodeCnx.createSegment(replicaSegment.build());
          break;
        }
      }

      // keep replica in sync
      onlineSegment = getOnlineReplicaMetas(metaBuilder, TimeUnit.MINUTES.toMillis(1));
      if (onlineSegment.size() == 0) {
        continue;
      }

      String mainAddress = null;
      SegmentMeta main = null;
      Set<String> oldFollowers = Collections.emptySet();
      for (Map.Entry<String, SegmentMeta> e : onlineSegment.entrySet()) {
        String addr = e.getKey();
        SegmentMeta meta = e.getValue();
        for (SegmentMeta.Replica replica : meta.getReplicasList()) {
          if ((replica.getFlag() & FOLLOWER_MARK) == 0 && e.getKey().equals(replica.getAddress())) {
            mainAddress = addr;
            main = meta;
            oldFollowers =
                meta.getReplicasList().stream()
                    .map(r -> r.getAddress())
                    .filter(a -> !a.equals(addr))
                    .collect(Collectors.toSet());
          }
        }
        if (main == null && getOffset(addr, meta) == metaBuilder.getEndOffset()) {
          mainAddress = addr;
          main = meta;
        }
      }

      if (main == null) {
        log.warn("[REPLICA_CORRUPT] cannot find intact replica {}", onlineSegment);
        continue;
      }

      Set<String> newFollowers = new HashSet<>();
      for (Map.Entry<String, SegmentMeta> e : onlineSegment.entrySet()) {
        String addr = e.getKey();
        SegmentMeta replicaMeta = e.getValue();
        if (!addr.equals(mainAddress)
            && getOffset(addr, replicaMeta) < metaBuilder.getEndOffset()) {
          newFollowers.add(addr);
        }
      }

      List<String> add = new ArrayList<>(newFollowers);
      add.removeAll(oldFollowers);
      List<String> delete = new ArrayList<>(oldFollowers);
      delete.removeAll(newFollowers);
      if (add.size() != 0 || delete.size() != 0) {
        SegmentMeta newMeta =
            metaBuilder
                .clone()
                .clearReplicas()
                .addReplicas(SegmentMeta.Replica.newBuilder().setAddress(mainAddress).build())
                .addAllReplicas(
                    newFollowers.stream()
                        .map(
                            f ->
                                SegmentMeta.Replica.newBuilder()
                                    .setAddress(f)
                                    .setFlag(FOLLOWER_MARK)
                                    .build())
                        .collect(Collectors.toList()))
                .build();

        log.info(
            "[REPLICA_SYNC]{},old={},new={}",
            segmentId(metaBuilder),
            TextFormat.shortDebugString(main),
            TextFormat.shortDebugString(newMeta));
        controlNodeCnx.updateSegmentMeta(mainAddress, newMeta);
      }
    }
  }

  static String segmentId(SegmentMeta.Builder meta) {
    return meta.getTopicId() + "@" + meta.getPartition() + "@" + meta.getIndex();
  }

  private static long getOffset(String address, SegmentMeta meta) {
    return meta.getReplicasList().stream()
        .filter(r -> r.getAddress().equals(address))
        .map(r -> r.getReplicaOffset())
        .findAny()
        .orElse(0L);
  }

  private Map<String, SegmentMeta> getOnlineReplicaMetas(
      SegmentMeta.Builder meta, long expiredMills) {
    SegmentKey key = new SegmentKey(meta.getTopicId(), meta.getPartition(), meta.getIndex());
    Map<String, Timestamped<SegmentMeta>> map = aliveSegments.get(key);
    Map<String, SegmentMeta> onlineSegment = new HashMap<>();
    map.forEach(
        (addr, seg) -> {
          if (seg.getTimestamp() + expiredMills > System.currentTimeMillis()) {
            onlineSegment.put(addr, seg.getData());
          }
        });
    return onlineSegment;
  }

  private Map<SegmentKey, SegmentMeta.Builder> loadSegments() {
    Map<SegmentKey, SegmentMeta.Builder> segments = new ConcurrentHashMap<>();
    try {
      kvStore
          .get("segments/")
          .thenAccept(
              r -> {
                r.getKvs().stream()
                    .forEach(
                        m -> {
                          SegmentMeta.Builder builder = SegmentMeta.newBuilder();
                          Utils.jsonBytes2pb(m.getValue().getBytes(), builder);
                          SegmentMeta meta = builder.build();
                          segments.put(
                              new SegmentKey(
                                  meta.getTopicId(), meta.getPartition(), meta.getIndex()),
                              meta.toBuilder());
                          log.info("[SEGMENT_META_LOAD]{}", TextFormat.shortDebugString(meta));
                        });
              })
          .get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return segments;
  }

  @Override
  public void register(NodeMeta nodeMeta, SegmentMeta segment) {
    checkLeadership();
    SegmentKey key =
        new SegmentKey(segment.getTopicId(), segment.getPartition(), segment.getIndex());
    Map<String, Timestamped<SegmentMeta>> map = aliveSegments.get(key);
    if (map == null) {
      aliveSegments.putIfAbsent(key, new ConcurrentHashMap<>());
      map = aliveSegments.get(key);
    }
    map.put(nodeMeta.getAddress(), new Timestamped<>(segment));
  }

  @Override
  public void create(
      SegmentManagerServiceProto.CreateRequest request,
      StreamObserver<SegmentManagerServiceProto.CreateResponse> responseObserver) {
    checkLeadership();
    TopicMeta topicMeta = partitionRegistry.getTopicMeta(request.getTopicId());
    int replicaNum = topicMeta.getReplicaNum();
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
    if (replicas.size() < replicaNum / 2 + 1) {
      log.warn(
          "[NO_ENOUGH_REPLICA] create segment {}-{}-{} fail",
          request.getTopicId(),
          request.getPartition(),
          request.getLastIndex() + 1);
      responseObserver.onNext(
          SegmentManagerServiceProto.CreateResponse.newBuilder()
              .setStatus(SegmentManagerServiceProto.CreateResponse.Status.FAIL)
              .build());
      responseObserver.onCompleted();
      return;
    }
    SegmentMeta.Builder builder =
        SegmentMeta.newBuilder()
            .setTopicId(request.getTopicId())
            .setPartition(request.getPartition())
            .setIndex(request.getLastIndex() + 1)
            .setReplicaNum(replicaNum);
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
                  String.format(
                      "segments/%s/%s/%s",
                      request.getTopicId(), request.getPartition(), request.getLastIndex() + 1),
                  Utils.pb2jsonBytes(builder.clone()));
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
              responseObserver.onError(t);
              return null;
            });
  }

  @Override
  public void getSegments(
      SegmentManagerServiceProto.GetSegmentsRequest request,
      StreamObserver<SegmentManagerServiceProto.GetSegmentsResponse> responseObserver) {
    checkLeadership();
    List<SegmentMeta.Builder> metas = new ArrayList<>();
    this.segments.forEach(
        (k, v) -> {
          if (request.getTopicId() == k.getTopicId()
              && request.getPartition() == k.getPartition()) {
            metas.add(v);
          }
        });
    Collections.sort(metas, Comparator.comparing(SegmentMeta.Builder::getIndex));
    responseObserver.onNext(
        SegmentManagerServiceProto.GetSegmentsResponse.newBuilder()
            .addAllSegments(
                metas.stream()
                    .map(b -> getSegment0(b.getTopicId(), b.getPartition(), b.getIndex()))
                    .collect(Collectors.toList()))
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getSegment(
      SegmentManagerServiceProto.GetSegmentRequest request,
      StreamObserver<SegmentManagerServiceProto.GetSegmentResponse> responseObserver) {
    checkLeadership();
    responseObserver.onNext(
        SegmentManagerServiceProto.GetSegmentResponse.newBuilder()
            .setMeta(getSegment0(request.getTopicId(), request.getPartition(), request.getIndex()))
            .build());
    responseObserver.onCompleted();
  }

  private SegmentMeta getSegment0(int topicId, int partition, int index) {
    SegmentMeta.Builder meta = segments.get(new SegmentKey(topicId, partition, index)).clone();
    Map<String, SegmentMeta> replicaMetas =
        getOnlineReplicaMetas(meta, TimeUnit.MINUTES.toMillis(1));
    List<SegmentMeta.Replica> replicas = new LinkedList<>();
    replicaMetas.forEach(
        (addr, replicaMeta) -> {
          Optional<SegmentMeta.Replica> replica =
              replicaMeta.getReplicasList().stream()
                  .filter(r -> r.getAddress().equals(addr))
                  .findAny();
          if (replica.isPresent()) {
            replicas.add(replica.get());
          }
        });
    if (replicas.size() != 0) {
      meta.clearReplicas().addAllReplicas(replicas);
    }
    return meta.build();
  }

  @Override
  public void seal(
      SegmentManagerServiceProto.SealRequest request,
      StreamObserver<SegmentManagerServiceProto.SealResponse> responseObserver) {
    checkLeadership();
    SegmentMeta.Builder meta =
        this.segments.get(
            new SegmentKey(request.getTopicId(), request.getPartition(), request.getIndex()));
    if ((meta.getFlag() & Segment.SEAL_MARK) != 0) {
      responseObserver.onNext(
          SegmentManagerServiceProto.SealResponse.newBuilder()
              .setEndOffset(meta.getEndOffset())
              .build());
      responseObserver.onCompleted();
      return;
    }
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
                                  log.warn(
                                      "[SEALFAIL] seal {}-{}-{} in {} fail",
                                      request.getTopicId(),
                                      request.getPartition(),
                                      request.getIndex(),
                                      r.getAddress(),
                                      throwable);
                                  endOffsets.add(NO_OFFSET);
                                  rst.complete(null);
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
              long endOffset = endOffsets.get(endOffsets.size() / 2);
              if (endOffset == NO_OFFSET) {
                log.warn("[SEAL_FAIL] no enough replica");
                responseObserver.onNext(
                    SegmentManagerServiceProto.SealResponse.newBuilder()
                        .setStatus(SegmentManagerServiceProto.SealResponse.Status.FAIL)
                        .build());
                responseObserver.onCompleted();
                return;
              }
              meta.setEndOffset(endOffset);
              meta.setFlag(meta.getFlag() | Segment.SEAL_MARK);
              log.info(
                  "[SEAL] {}-{}-{} with endOffset {}",
                  request.getTopicId(),
                  request.getPartition(),
                  request.getIndex(),
                  endOffset);
              saveSegmentMeta(meta.build());
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

  private void saveSegmentMeta(SegmentMeta meta) {
    kvStore.put(
        String.format("segments/%s/%s/%s", meta.getTopicId(), meta.getPartition(), meta.getIndex()),
        Utils.pb2jsonBytes(meta.toBuilder().clone()));
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

  public void setPartitionRegistry(PartitionRegistry partitionRegistry) {
    this.partitionRegistry = partitionRegistry;
  }

  public void setElection(LinkyElection election) {
    this.election = election;
  }

  @Override
  public synchronized void onChanged(LinkyElection.Leader leader) {
    if (leader.isCurrentNode()) {
      this.segments = loadSegments();
    } else {
    }
  }

  private void checkLeadership() {
    if (!election.isLeader()) {
      throw new LinkyIOException("NOT LEADER");
    }
  }
}
