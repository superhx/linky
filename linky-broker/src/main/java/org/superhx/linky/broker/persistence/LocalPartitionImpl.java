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
package org.superhx.linky.broker.persistence;

import java.util.concurrent.CompletableFuture;

import org.superhx.linky.service.proto.BatchRecord;

public class LocalPartitionImpl implements Partition {
    private PersistenceFactory factory;
    private volatile Segment   lastSegment;

    public LocalPartitionImpl(PersistenceFactory factory) {
        this.factory = factory;
    }

    @Override
    public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
        Segment segment = getLastSegment();
        CompletableFuture<Segment.AppendResult> segmentAppendFuture = segment.append(batchRecord);
        return segmentAppendFuture.handle((appendResult, throwable) -> {
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            return new AppendResult(appendResult.getOffset());
        });
    }

    @Override
    public CompletableFuture<BatchRecord> get(long offset) {
        Segment segment = getLastSegment();
        CompletableFuture<BatchRecord> segmentGetFuture = segment.get(offset);
        return segmentGetFuture.handle((batchRecord, throwable) -> {
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            return batchRecord;
        });
    }

    private Segment getLastSegment() {
        if (lastSegment != null) {
            return lastSegment;
        }
        synchronized (this) {
            if (lastSegment != null) {
                return lastSegment;
            }
            lastSegment = this.factory.newSegment();
        }
        return lastSegment;
    }
}
