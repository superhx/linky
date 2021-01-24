package org.superhx.linky.broker.persistence;

import org.superhx.linky.broker.Lifecycle;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface Journal extends Lifecycle {

  void append(BytesData bytesData, Consumer<AppendResult> callback);

  CompletableFuture<Record> get(long offset, int size);

  CompletableFuture<Record> get(long offset);

  long getStartOffset();

  long getConfirmOffset();

  void delete();

  String getPath();

  JournalLog getJournalLog(long offset);

  void reclaimSpace(JournalLog log);

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

  class Record {
    private long offset;
    private int size;
    private boolean blank;
    private BytesData data;

    public BytesData getData() {
      return data;
    }

    public long getOffset() {
      return offset;
    }

    public int getSize() {
      return size;
    }

    public boolean isBlank() {
      return blank;
    }

    public static Builder newBuilder() {
      return new Builder();
    }
  }

  class Builder {
    private long offset;
    private int size;
    private boolean blank;
    private BytesData data;

    public Builder setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder setSize(int size) {
      this.size = size;
      return this;
    }

    public Builder setBlank(boolean blank) {
      this.blank = blank;
      return this;
    }

    public Builder setData(BytesData data) {
      this.data = data;
      return this;
    }

    public Record build() {
      Record record = new Record();
      record.offset = offset;
      record.size = size;
      record.blank = blank;
      record.data = data;
      return record;
    }
  }

  class BytesData {
    private byte[] bytes;

    public BytesData(byte[] bytes) {
      this.bytes = bytes;
    }

    public byte[] toByteArray() {
      return bytes;
    }
  }
}
