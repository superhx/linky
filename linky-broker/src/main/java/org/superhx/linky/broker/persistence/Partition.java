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

import io.grpc.stub.StreamObserver;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.PartitionMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Partition {

  CompletableFuture<AppendResult> append(BatchRecord batchRecord);

  CompletableFuture<GetResult> get(byte[] cursor);

  CompletableFuture<GetResult> get(byte[] cursor, boolean skipInvisible, boolean link);

  void get(byte[] startCursor, byte[] endCursor, StreamObserver<BatchRecord> stream);

  CompletableFuture<GetKVResult> getKV(byte[] key, boolean meta);

  CompletableFuture<PartitionStatus> open();

  CompletableFuture<Void> close();

  PartitionStatus status();

  Cursor getNextCursor();

  CompletableFuture<List<TimerIndex>> getTimerSlot(Cursor cursor);

  PartitionMeta meta();

  String name();

  byte[] getMeta(byte[] key);

  CompletableFuture<Void> setMeta(byte[]... kv);

  AppendPipeline appendPipeline();

  void registerAppendHook(AppendHook appendHook);

  interface AppendHook {
    void before(AppendContext ctx, BatchRecord.Builder batchRecord);

    void after(AppendContext ctx);
  }

  class AppendContext {
    public static final String TIMER_INDEX_CTX_KEY = "TIMER_INDEX_CTX_KEY";
    public static final String TIMESTAMP_CTX_KEY = "TIMESTAMP_CTX_KEY";
    public static final String TRANS_MSG_CTX_KEY = "TRANS_MSG_CTX_KEY";
    public static final String TRANS_CONFIRM_CTX_KEY = "TRANS_CONFIRM_CTX_KEY";

    private Map<String, Object> contexts;
    private Cursor cursor;
    private Cursor nextCursor;

    public <T> T getContext(String name) {
      if (contexts == null) {
        return null;
      }
      return (T) contexts.get(name);
    }

    public void putContext(String name, Object obj) {
      if (contexts == null) {
        contexts = new HashMap<>();
      }
      contexts.put(name, obj);
    }

    public Cursor getCursor() {
      return cursor;
    }

    public void setCursor(Cursor cursor) {
      this.cursor = cursor;
    }

    public Cursor getNextCursor() {
      return nextCursor;
    }

    public void setNextCursor(Cursor nextCursor) {
      this.nextCursor = nextCursor;
    }
  }

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

    public GetResult setBatchRecord(BatchRecord batchRecord) {
      this.batchRecord = batchRecord;
      return this;
    }

    public byte[] getNextCursor() {
      return nextCursor;
    }

    public GetResult setNextCursor(byte[] nextCursor) {
      this.nextCursor = nextCursor;
      return this;
    }

    public GetStatus getStatus() {
      return status;
    }

    public GetResult setStatus(GetStatus status) {
      this.status = status;
      return this;
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
}
