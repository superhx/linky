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
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import org.superhx.linky.broker.Utils;

import java.util.concurrent.CompletableFuture;

public class KVStore {
  private String namespace = "/linky/";
  private Client client;
  private KV kvClient;

  public KVStore() {
    this.client = Client.builder().endpoints("http://localhost:2379").build();
    this.kvClient = this.client.getKVClient();
  }

  public CompletableFuture<PutResponse> put(String key, byte[] value) {
    return kvClient.put(
        ByteSequence.from((namespace + key).getBytes(Utils.DEFAULT_CHARSET)),
        ByteSequence.from(value));
  }

  public CompletableFuture<GetResponse> get(String key) {
    ByteSequence prefix = ByteSequence.from((namespace + key).getBytes(Utils.DEFAULT_CHARSET));
    return kvClient.get(prefix, GetOption.newBuilder().withPrefix(prefix).build());
  }
}
