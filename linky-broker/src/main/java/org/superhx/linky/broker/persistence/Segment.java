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

import java.util.concurrent.CompletableFuture;

public interface Segment extends Lifecycle {
  int FOLLOWER_MARK = 1 << 0;
  int SEAL_MARK = 1 << 1;
  int NO_INDEX = -1;
  long NO_OFFSET = -1L;

  CompletableFuture<AppendResult> append(AppendContext ctx, BatchRecord batchRecord);

  CompletableFuture<BatchRecord> get(long offset);

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
    private long offset;

    public AppendResult(Status status, long offset) {
      this.status = status;
      this.offset = offset;
    }

    public AppendResult(long offset) {
      this.offset = offset;
    }

    public long getOffset() {
      return offset;
    }

    public Status getStatus() {
      return this.status;
    }
  }

  class AppendContext {
    private int term;

    public int getTerm() {
      return term;
    }

    public void setTerm(int term) {
      this.term = term;
    }
  }

  enum Status {
    WRITABLE,
    REPLICA_LOSS,
    REPLICA_BREAK,
    SEALED
  }
}
