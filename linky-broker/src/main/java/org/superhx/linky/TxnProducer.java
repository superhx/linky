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
import org.superhx.linky.broker.persistence.Constants;
import org.superhx.linky.service.proto.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class TxnProducer {
  static int count = 1;

  public static void main(String... args) throws InterruptedException {
//    prepare();
      confirm(false, 1, 2);
  }

  public static void prepare() throws InterruptedException {
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
              .addRecords(
                  Record.newBuilder()
                      .setKey(ByteString.copyFrom("rk", Charset.forName("UTF-8")))
                      .setValue(ByteString.copyFrom((new Date() + body).getBytes()))
                      .build())
              .setFlag(Constants.TRANS_MSG_FLAG)
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
                      "req return %s seg %s segOff %s",
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
  }

  public static void confirm(boolean commit, int index, long offset) throws InterruptedException {
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
              .addRecords(
                  Record.newBuilder()
                      .setValue(
                          ByteString.copyFrom(
                              ByteBuffer.allocate(1 + 4 + 8).put(commit ? (byte) 0: (byte)1).putInt(index).putLong(offset).array()))
                      .build())
              .setFlag(Constants.TRANS_CONFIRM_FLAG)
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
                      "req return %s seg %s segOff %s",
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
  }
}
