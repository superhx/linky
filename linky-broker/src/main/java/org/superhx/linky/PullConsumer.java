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
import org.superhx.linky.service.proto.GetRequest;
import org.superhx.linky.service.proto.GetResponse;
import org.superhx.linky.service.proto.RecordServiceGrpc;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PullConsumer {
  public static void main(String... args) throws InterruptedException {

    ManagedChannel channel =
        ManagedChannelBuilder.forTarget("localhost:9591").usePlaintext().build();
    RecordServiceGrpc.RecordServiceStub stub = RecordServiceGrpc.newStub(channel);
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
}
