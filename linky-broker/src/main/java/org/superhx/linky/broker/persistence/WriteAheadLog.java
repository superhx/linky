package org.superhx.linky.broker.persistence;

import java.util.concurrent.CompletableFuture;

import org.superhx.linky.service.proto.BatchRecord;

public interface WriteAheadLog {

    CompletableFuture<AppendResult> append(BatchRecord batchRecord);

    CompletableFuture<BatchRecord> get(long offset);

    class AppendResult {
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
}
