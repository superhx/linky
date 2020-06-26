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

import java.io.IOException;

import org.superhx.linky.broker.persistence.LocalPersistenceFactoryImpl;
import org.superhx.linky.broker.persistence.Partition;
import org.superhx.linky.service.proto.*;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class LinkyBrokerStartup {
    Partition partition;
    Server    server;

    public LinkyBrokerStartup() {
        partition = new LocalPersistenceFactoryImpl().newPartition();
        System.out.println("first linky event");
        server = ServerBuilder.forPort(9594).addService(new RecordService()).build();
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        server.awaitTermination();
    }

    public static void main(String... args) throws IOException, InterruptedException {
        new LinkyBrokerStartup().start();
    }

    class RecordService extends RecordServiceGrpc.RecordServiceImplBase {

        @Override
        public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
            partition.append(request.getBatchRecord()).thenAccept(appendResult -> {
                PutResponse response = PutResponse.newBuilder().setStatus(PutResponse.Status.SUCCESS)
                    .setOffset(appendResult.getOffset()).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            });
        }

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            partition.get(request.getOffset()).thenAccept(getResult -> {
                GetResponse response = GetResponse.newBuilder().setBatchRecord(getResult).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            });
        }
    }
}
