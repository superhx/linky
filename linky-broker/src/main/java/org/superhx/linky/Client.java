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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Client {
  static int count = 1;
  //    static int count = 1;
  //    static int count = 0;

  public static void main(String... args) throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:9591").usePlaintext().build();
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
              ByteString bs = putResponse.getCursor();
              ByteBuffer buf = ByteBuffer.wrap(bs.toByteArray());
              System.out.println(
                  String.format(
                      "req return %s seg %s segOff", putResponse, buf.getInt(), buf.getLong()));
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

    AtomicReference<byte[]> cursor = new AtomicReference<>(new byte[4 + 8]);
    for (int i = 0; i < 2; i++) {
      CountDownLatch getLatch = new CountDownLatch(1);
      stub.get(
          GetRequest.newBuilder()
              .setTopic("FOO")
              .setPartition(0)
              .setCursor(ByteString.copyFrom(cursor.get()))
              .build(),
          new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse getResponse) {
              ByteBuffer buf = ByteBuffer.wrap(getResponse.getNextCursor().toByteArray());
              System.out.println(
                  "Get return offset:"
                      + getResponse.getBatchRecord().getFirstOffset()
                      + " count:"
                      + getResponse.getBatchRecord().getRecordsCount()
                      + " record:"
                      + getResponse.getBatchRecord()
                      + " next: seg "
                      + buf.getInt()
                      + " segOffset "
                      + buf.getLong());
              cursor.set(buf.array());
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              getLatch.countDown();
            }
          });
      getLatch.await();
    }
    channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
  }
}
