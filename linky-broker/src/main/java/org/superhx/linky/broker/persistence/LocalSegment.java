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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.superhx.linky.service.proto.BatchRecord;

public class LocalSegment implements Segment {
    private WriteAheadLog   wal;
    private AtomicLong      nextOffset;
    private long            startOffset;
    private long            endOffset;
    private Map<Long, Long> indexMap;

    public LocalSegment(long startOffset, WriteAheadLog wal) {
        this.startOffset = startOffset;
        this.nextOffset = new AtomicLong(this.startOffset);
        this.wal = wal;

        indexMap = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
        int recordsCount = batchRecord.getRecordsCount();
        long offset = nextOffset.getAndAdd(recordsCount);
        batchRecord = BatchRecord.newBuilder(batchRecord).setFirstOffset(offset).build();
        return wal.append(batchRecord).handle((appendResult, throwable) -> {
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            indexMap.put(offset, appendResult.getOffset());
            return new AppendResult(offset);
        });
    }

    @Override
    public CompletableFuture<BatchRecord> get(long offset) {
        long physicalOffset = indexMap.get(offset);
        return wal.get(physicalOffset).handle((batchRecord, throwable) -> batchRecord);
    }

    @Override
    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public void setStartOffset(long offset) {
        this.startOffset = offset;
    }

    @Override
    public long getEndOffset() {
        return this.endOffset;
    }

    @Override
    public void setEndOffset(long offset) {
        this.endOffset = offset;
    }

    public void setWal(WriteAheadLog wal) {
        this.wal = wal;
    }
}
