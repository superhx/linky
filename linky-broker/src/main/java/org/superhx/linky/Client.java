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

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
  static int count = 1;
  //    static int count = 1;
  //    static int count = 0;

  public static void main(String... args) throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:9593").usePlaintext().build();
    RecordServiceGrpc.RecordServiceStub stub = RecordServiceGrpc.newStub(channel);
    final AtomicLong maxOffset = new AtomicLong();
    long start = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      String body = "";
      //      for (int j = 0; j < 1024; j++) {
      //        body += "hello";
      //      }
      BatchRecord batchRecord =
          BatchRecord.newBuilder()
              .setPartition(0)
              .addRecords(
                  Record.newBuilder()
                      .setKey("rk")
                      .setValue(ByteString.copyFrom((new Date() + body).getBytes()))
                      .build())
              .build();
      PutRequest request =
          PutRequest.newBuilder().setTopic("FOO").setBatchRecord(batchRecord).build();
      CountDownLatch finalLatch1 = latch;
      stub.put(
          request,
          new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {
              System.out.println(String.format("req return %s", putResponse));
              if (maxOffset.get() < putResponse.getOffset() + 1) {
                maxOffset.set(putResponse.getOffset() + 1);
              }
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              finalLatch1.countDown();
            }
          });
    }
    latch.await();
    System.out.println(
        String.format("send %s message cost %s ms", count, System.currentTimeMillis() - start));

    for (int i = 0; i < maxOffset.get(); i++) {
      //          for (int i = 202; i < 203; i++) {

      latch = new CountDownLatch(1);
      CountDownLatch finalLatch = latch;
      stub.get(
          GetRequest.newBuilder().setTopic("FOO").setPartition(0).setOffset(i).build(),
          new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse getResponse) {
              System.out.println(
                  "Get return offset:"
                      + getResponse.getBatchRecord().getFirstOffset()
                      + " count:"
                      + getResponse.getBatchRecord().getRecordsCount() + " record:" + getResponse.getBatchRecord());
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              finalLatch.countDown();
            }
          });
      latch.await();
    }
    channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
  }
}
