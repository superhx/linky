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
import org.superhx.linky.data.service.proto.SegmentServiceProto;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.SegmentMeta;

import java.util.concurrent.CompletableFuture;

public interface Segment {
  int FOLLOWER_MARK = 1 << 0;
  int SEAL_MARK = 1 << 1;
  int NO_INDEX = -1;
  long NO_OFFSET = -1L;

  CompletableFuture<AppendResult> append(BatchRecord batchRecord);

  CompletableFuture<BatchRecord> get(long offset);

  default CompletableFuture<SegmentServiceProto.ReplicateResponse> replicate(
      SegmentServiceProto.ReplicateRequest request) {
    throw new UnsupportedOperationException();
  }

  default void syncCmd(SegmentServiceProto.SyncCmdRequest request, StreamObserver<SegmentServiceProto.SyncCmdResponse> responseObserver) {
      throw new UnsupportedOperationException();
  }

  default void sync(
      SegmentServiceProto.SyncRequest request,
      StreamObserver<SegmentServiceProto.SyncResponse> responseObserver) {
    throw new UnsupportedOperationException();
  }

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

  SegmentMeta getMeta();

  CompletableFuture<Void> seal();

  CompletableFuture<Void> seal0();

  default Status getStatus() {
    return Status.WRITABLE;
  }

  class AppendResult {
    enum Status {
      SUCCESS,
      REPLICA_LOSS,
      REPLICA_BREAK
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

    public void setOffset(long offset) {
      this.offset = offset;
    }
  }
  //
  //  class ReplicateResult {
  //
  //    public enum Status {
  //      SUCCESS,
  //      RESET
  //    }
  //
  //    private Status status = Status.SUCCESS;
  //    private long confirmOffset;
  //
  //    public ReplicateResult(long confirmOffset) {
  //      this.confirmOffset = confirmOffset;
  //    }
  //
  //    public ReplicateResult(ReplicateResult.Status status, long confirmOffset) {
  //        this.status = status;
  //        this.confirmOffset = confirmOffset;
  //    }
  //
  //    public long getConfirmOffset() {
  //      return confirmOffset;
  //    }
  //
  //    public void setConfirmOffset(long confirmOffset) {
  //      this.confirmOffset = confirmOffset;
  //    }
  //
  //    public Status getStatus() {
  //      return status;
  //    }
  //
  //    public void setStatus(Status status) {
  //      this.status = status;
  //    }
  //  }

  enum Status {
    WRITABLE,
    REPLICA_LOSS,
    READONLY,
    FULL,
    REPLICA_BREAK
  }
}
