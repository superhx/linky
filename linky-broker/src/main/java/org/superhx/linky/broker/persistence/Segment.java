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

import org.superhx.linky.service.proto.BatchRecord;

import java.util.concurrent.CompletableFuture;

public interface Segment {
    int NO_INDEX = -1;
    long NO_OFFSET = -1L;

    CompletableFuture<AppendResult> append(BatchRecord batchRecord);

    CompletableFuture<ReplicateResult> replicate(BatchRecord batchRecord);

    CompletableFuture<BatchRecord> get(long offset);

    int getIndex();

    /**
     * get inclusive relative start offset
     *
     * @return
     */
    long getStartOffset();

    /**
     * set inclusive relative start offset
     *
     * @param offset
     */
    void setStartOffset(long offset);

    /**
     * get exclusive relative end offset
     *
     * @return offset
     */
    long getEndOffset();

    /**
     * set exclusive relative end offset
     *
     * @param offset
     */
    void setEndOffset(long offset);

    CompletableFuture<Void> seal();

    class AppendResult {
        private Status status = Status.SUCCESS;
        private long offset;

        public AppendResult(long offset) {
            this.offset = offset;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }
    }

    class ReplicateResult {
        private Status status = Status.SUCCESS;
        private long confirmOffset;

        public ReplicateResult(long confirmOffset) {
            this.confirmOffset = confirmOffset;
        }

        public long getConfirmOffset() {
            return confirmOffset;
        }

        public void setConfirmOffset(long confirmOffset) {
            this.confirmOffset = confirmOffset;
        }
    }

    enum Status {
        SUCCESS;
    }
}