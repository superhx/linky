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

public interface Partition {

    CompletableFuture<AppendResult> append(BatchRecord batchRecord);

    CompletableFuture<BatchRecord> get(long offset);

    class AppendResult {
        private Status status = Status.SUCCESS;
        private long   offset;

        public AppendResult(long offset) {
            this.offset = offset;
        }

        public Status getStatus() {
            return status;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }
    }

    enum Status {
                 SUCCESS;
    }
}
