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
import org.superhx.linky.service.proto.PartitionMeta;

import java.util.concurrent.CompletableFuture;

public interface Partition {

  CompletableFuture<AppendResult> append(BatchRecord batchRecord);

  CompletableFuture<GetResult> get(byte[] cursor);

  CompletableFuture<PartitionStatus> open();

  CompletableFuture<Void> close();

  PartitionStatus status();

  PartitionMeta meta();

  class AppendResult {
    private AppendStatus status = AppendStatus.SUCCESS;
    private byte[] cursor;

    public AppendResult(byte[] cursor) {
      this.cursor = cursor;
    }

    public AppendStatus getStatus() {
      return status;
    }

    public void setStatus(AppendStatus status) {
      this.status = status;
    }

    public byte[] getCursor() {
      return cursor;
    }

    public void setCursor(byte[] cursor) {
      this.cursor = cursor;
    }
  }

  enum AppendStatus {
    SUCCESS;
  }

  class GetResult {
    private BatchRecord batchRecord;
    private byte[] nextCursor;
    private GetStatus status = GetStatus.SUCCESS;

    public BatchRecord getBatchRecord() {
      return batchRecord;
    }

    public void setBatchRecord(BatchRecord batchRecord) {
      this.batchRecord = batchRecord;
    }

    public byte[] getNextCursor() {
      return nextCursor;
    }

    public void setNextCursor(byte[] nextCursor) {
      this.nextCursor = nextCursor;
    }

    public GetStatus getStatus() {
      return status;
    }

    public void setStatus(GetStatus status) {
      this.status = status;
    }
  }

  enum GetStatus {
    SUCCESS,
    NO_NEW_MSG;
  }

  enum PartitionStatus {
    NOOP,
    OPENING,
    OPEN,
    SHUTTING,
    SHUTDOWN,
    ERROR
  }
}
