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
import com.google.protobuf.TextFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.superhx.linky.service.proto.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Client {
  static int count = 0;

  public static void main(String... args) throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:9591").usePlaintext().build();
    RecordServiceGrpc.RecordServiceStub stub = RecordServiceGrpc.newStub(channel);
    long start = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      String body = "";
      BatchRecord batchRecord =
          BatchRecord.newBuilder()
              .setPartition(0)
              .setVisibleTimestamp(
                  LocalDateTime.parse("2021-03-01T21:28:10.00").toEpochSecond(ZoneOffset.of("+8"))
                      * 1000L)
              .addRecords(
                  Record.newBuilder()
                      .setKey(ByteString.copyFrom("rk", Charset.forName("UTF-8")))
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
                      "req return %s seg %s segOff",
                      TextFormat.shortDebugString(putResponse), buf.getInt(), buf.getLong()));
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

    getkv(stub, "rk");
    //    getkv(stub, "rk1");

    AtomicReference<byte[]> cursor = new AtomicReference<>(new byte[4 + 8]);
    final boolean[] end = {false};
    for (int i = 0; i < 100; i++) {
      if (end[0]) {
        break;
      }
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
              if (getResponse.getStatus() == GetResponse.Status.NO_NEW_MSG) {
                end[0] = true;
                return;
              }
              ByteBuffer buf = ByteBuffer.wrap(getResponse.getNextCursor().toByteArray());
              System.out.println(
                  "Get return offset:"
                      + getResponse.getBatchRecord().getFirstOffset()
                      + " count:"
                      + getResponse.getBatchRecord().getRecordsCount()
                      + " next: seg "
                      + buf.getInt()
                      + " segOffset "
                      + buf.getLong()
                      + " body"
                      + TextFormat.shortDebugString(getResponse.getBatchRecord()));
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
  //
  private static void getkv(RecordServiceGrpc.RecordServiceStub stub, String key)
      throws InterruptedException {
    CountDownLatch getKvLatch = new CountDownLatch(1);
    stub.getKV(
        GetKVRequest.newBuilder()
            .setTopic("FOO")
            .setPartition(0)
            .setKey(ByteString.copyFrom(key, Charset.forName("UTF-8")))
            .build(),
        new StreamObserver<GetKVResponse>() {
          @Override
          public void onNext(GetKVResponse getKVResponse) {
            System.out.println(
                "getkv " + TextFormat.shortDebugString(getKVResponse.getBatchRecord()));
            getKvLatch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            throwable.printStackTrace();
            getKvLatch.countDown();
          }

          @Override
          public void onCompleted() {}
        });
    getKvLatch.await();
  }
}
