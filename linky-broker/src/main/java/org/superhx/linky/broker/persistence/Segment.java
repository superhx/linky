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
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Segment extends Lifecycle {
  int FOLLOWER_MARK = 1 << 0;
  int SEAL_MARK = 1 << 1;
  int NO_INDEX = -1;
  long NO_OFFSET = -1L;

  CompletableFuture<AppendResult> append(AppendContext context, BatchRecord batchRecord);

  CompletableFuture<BatchRecord> get(long offset);

  CompletableFuture<BatchRecord> getKV(byte[] key, boolean meta);

  CompletableFuture<List<BatchRecord>> getTimerSlot(long offset);

  default void replicate(
      SegmentServiceProto.ReplicateRequest request,
      StreamObserver<SegmentServiceProto.ReplicateResponse> responseObserver) {
    throw new UnsupportedOperationException();
  }

  int getIndex();

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

  long getConfirmOffset();

  long getNextOffset();

  /**
   * reclaim space before exclusive offset
   *
   * @param offset
   */
  CompletableFuture<Void> reclaimSpace(long offset);

  SegmentMeta getMeta();

  CompletableFuture<Void> seal();

  boolean isSealed();

  void updateMeta(SegmentMeta meta);

  default Status getStatus() {
    return Status.SEALED;
  }

  class AppendResult {
    enum Status {
      SUCCESS,
      REPLICA_BREAK,
      SEALED
    }

    private Status status = Status.SUCCESS;
    private int index;
    private long offset;

    public AppendResult(Status status) {
      this.status = status;
    }

    public AppendResult(int index, long offset) {
      this.index = index;
      this.offset = offset;
    }

    public int getIndex() {
      return index;
    }

    public long getOffset() {
      return offset;
    }

    public Status getStatus() {
      return this.status;
    }
  }

  class AppendContext {
    private static final AppendHook NOOP_HOOK =
        new AppendHook() {
          @Override
          public void before(AppendContext context, BatchRecord record) {}

          @Override
          public void after(AppendContext context, BatchRecord record) {}
        };

    private AppendHook hook = NOOP_HOOK;
    private int index;
    private long offset;

    public AppendHook getHook() {
      return hook;
    }

    public AppendContext setHook(AppendHook hook) {
      this.hook = hook;
      return this;
    }

    public int getIndex() {
      return index;
    }

    public AppendContext setIndex(int index) {
      this.index = index;
      return this;
    }

    public long getOffset() {
      return offset;
    }

    public AppendContext setOffset(long offset) {
      this.offset = offset;
      return this;
    }
  }

  interface AppendHook {
    default void before(AppendContext context, BatchRecord record) {}

    default void after(AppendContext context, BatchRecord record) {}
  }

  enum Status {
    WRITABLE,
    REPLICA_BREAK,
    SEALED
  }
}
