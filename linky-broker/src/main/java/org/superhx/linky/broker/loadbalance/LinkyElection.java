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

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.election.LeaderResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.BrokerContext;
import org.superhx.linky.broker.Utils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class LinkyElection {
  private static final Logger log = LoggerFactory.getLogger(LinkyElection.class);
  private BrokerContext brokerContext;
  private Client client;
  private Lease lease;
  private Election election;
  private ByteSequence electionName =
      ByteSequence.from("/linky/controller/node", Utils.DEFAULT_CHARSET);
  private List<LeaderChangeListener> listeners = new CopyOnWriteArrayList<>();
  private Leader leader = new Leader(null, -1);

  public LinkyElection(BrokerContext brokerContext) {
    this.brokerContext = brokerContext;
    this.client = Client.builder().endpoints("http://localhost:2379").build();
    lease = this.client.getLeaseClient();
    election = this.client.getElectionClient();
    lease
        .grant(10)
        .thenAccept(
            l -> {
              long leaseId = l.getID();
              lease.keepAlive(
                  leaseId,
                  new StreamObserver<LeaseKeepAliveResponse>() {
                    @Override
                    public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {}

                    @Override
                    public void onError(Throwable throwable) {}

                    @Override
                    public void onCompleted() {}
                  });
              election.observe(
                  electionName,
                  new Election.Listener() {
                    @Override
                    public void onNext(LeaderResponse response) {
                      Leader newLeader =
                          new Leader(
                              response.getKv().getValue().toString(Utils.DEFAULT_CHARSET),
                              response.getHeader().getRevision());
                      log.info("controller node change from {} to {}", leader, newLeader);
                      leader = newLeader;
                      for (LeaderChangeListener listener : listeners) {
                        listener.onChanged(leader);
                      }
                    }

                    @Override
                    public void onError(Throwable throwable) {}

                    @Override
                    public void onCompleted() {}
                  });
              election
                  .campaign(
                      electionName,
                      leaseId,
                      ByteSequence.from(brokerContext.getAddress(), Utils.DEFAULT_CHARSET))
                  .thenAccept(
                      c -> {
                        log.info("campaign success {}", c);
                      });
            });
  }

  public void registerListener(LeaderChangeListener listener) {
    listeners.add(listener);
    listener.onChanged(leader);
  }

  public boolean isLeader() {
    return this.brokerContext.getAddress().equals(this.leader.getAddress());
  }

  public Leader getLeader() {
    return this.leader;
  }

  interface LeaderChangeListener {
    void onChanged(Leader leader);
  }

  public static class Leader {
    private String address;
    private long epoch;

    public Leader(String address, long epoch) {
      this.address = address;
      this.epoch = epoch;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    public long getEpoch() {
      return epoch;
    }

    public void setEpoch(long epoch) {
      this.epoch = epoch;
    }
  }
}
