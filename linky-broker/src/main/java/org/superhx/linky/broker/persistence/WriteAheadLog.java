package org.superhx.linky.broker.persistence;

import org.superhx.linky.service.proto.BatchRecord;

import java.util.concurrent.CompletableFuture;

public interface WriteAheadLog {

  CompletableFuture<AppendResult> append(BatchRecord batchRecord);

  CompletableFuture<BatchRecord> get(long offset, int size);

  class AppendResult {
    private long offset;
    private int size;

    public AppendResult(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }
  }
}
