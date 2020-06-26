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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.superhx.linky.service.proto.*;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class Client {
    public static void main(String... args) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:9594").usePlaintext().build();
        RecordServiceGrpc.RecordServiceStub stub = RecordServiceGrpc.newStub(channel);
        BatchRecord batchRecord = BatchRecord.newBuilder().setPartition(0)
            .addRecords(
                Record.newBuilder().setKey("rk").setValue(ByteString.copyFrom("hello world".getBytes())).build())
            .build();
        PutRequest request = PutRequest.newBuilder().setTopic("TP_FOO").setBatchRecord(batchRecord).build();
        CountDownLatch latch = new CountDownLatch(1);
        stub.put(request, new StreamObserver<PutResponse>() {
            @Override
            public void onNext(PutResponse putResponse) {
                System.out.println(String.format("req %s return %s", request, putResponse));
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println(String.format("req %s complete", request));
                latch.countDown();
            }
        });

        latch.await();
        channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
    }
}
