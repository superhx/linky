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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.superhx.linky.service.proto.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Client {
  public static void main(String... args) throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:9594").usePlaintext().build();
    RecordServiceGrpc.RecordServiceStub stub = RecordServiceGrpc.newStub(channel);
    for (int i = 0; i < 5; i++) {
      BatchRecord batchRecord =
          BatchRecord.newBuilder()
              .setPartition(0)
              .addRecords(
                  Record.newBuilder()
                      .setKey("rk")
                      .setValue(ByteString.copyFrom(("hello world " + i).getBytes()))
                      .build())
              .build();
      PutRequest request =
          PutRequest.newBuilder().setTopic("FOO").setBatchRecord(batchRecord).build();
      CountDownLatch latch = new CountDownLatch(1);
      stub.put(
          request,
          new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {
              System.out.println(String.format("req %s return %s", request, putResponse));
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              latch.countDown();
            }
          });

      latch.await();
    }

    for (int i = 0; i < 5; i++) {
      CountDownLatch latch = new CountDownLatch(1);

      stub.get(
          GetRequest.newBuilder().setTopic("FOO").setPartition(0).setOffset(i).build(),
          new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse getResponse) {
              System.out.println("Get return " + getResponse);
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              latch.countDown();
            }
          });
      latch.await();
    }
    channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
  }
}
