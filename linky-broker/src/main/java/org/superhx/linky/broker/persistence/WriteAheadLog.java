package org.superhx.linky.broker.persistence;

import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.service.proto.BatchRecord;

import java.util.concurrent.CompletableFuture;

public interface WriteAheadLog extends Lifecycle {

  AppendResult append(BatchRecord batchRecord);

  CompletableFuture<BatchRecord> get(long offset, int size);

  CompletableFuture<WalBatchRecord> get(long offset);

  void registerAppendHook(AppendHook hook);

  long getStartOffset();

  long getConfirmOffset();

  class AppendResult extends CompletableFuture<AppendResult> {
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

    public void complete() {
      super.complete(this);
    }
  }

  class WalBatchRecord {
    private BatchRecord batchRecord;
    private int size;

    public WalBatchRecord(BatchRecord batchRecord, int size) {
      this.batchRecord = batchRecord;
      this.size = size;
    }

    public BatchRecord getBatchRecord() {
      return batchRecord;
    }

    public void setBatchRecord(BatchRecord batchRecord) {
      this.batchRecord = batchRecord;
    }

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }
  }

  interface AppendHook {
    void afterAppend(BatchRecord batchRecord, AppendResult appendResult);
  }
}
