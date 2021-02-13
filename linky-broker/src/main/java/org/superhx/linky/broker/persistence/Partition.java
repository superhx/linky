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

  CompletableFuture<GetKVResult> getKV(byte[] key, boolean meta);

  CompletableFuture<PartitionStatus> open();

  CompletableFuture<Void> close();

  PartitionStatus status();

  PartitionMeta meta();

  enum AppendStatus {
    SUCCESS,
    FAIL
  }

  class AppendResult {
    private AppendStatus status = AppendStatus.SUCCESS;
    private byte[] cursor;

    public AppendResult(byte[] cursor) {
      this.cursor = cursor;
    }

    public AppendResult(AppendStatus status) {
      this.status = status;
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

  class GetKVResult {
    private BatchRecord batchRecord;

    public GetKVResult() {}

    public GetKVResult(BatchRecord batchRecord) {
      this.batchRecord = batchRecord;
    }

    public BatchRecord getBatchRecord() {
      return batchRecord;
    }

    public void setBatchRecord(BatchRecord batchRecord) {
      this.batchRecord = batchRecord;
    }
  }

  enum PartitionStatus {
    NOOP,
    OPENING,
    OPEN,
    SHUTTING,
    SHUTDOWN,
    ERROR
  }

  class TimerIndex {
    private long timestamp;
    private int index;
    private long offset;

    public long getTimestamp() {
      return timestamp;
    }

    public TimerIndex setTimestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public int getIndex() {
      return index;
    }

    public TimerIndex setIndex(int index) {
      this.index = index;
      return this;
    }

    public long getOffset() {
      return offset;
    }

    public TimerIndex setOffset(long offset) {
      this.offset = offset;
      return this;
    }
  }

  class Cursor {
    private int index;
    private long offset;

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }
  }
}
