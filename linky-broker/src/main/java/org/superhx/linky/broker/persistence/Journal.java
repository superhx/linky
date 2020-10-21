package org.superhx.linky.broker.persistence;

import org.superhx.linky.broker.Lifecycle;

import java.util.concurrent.CompletableFuture;

public interface Journal<T extends Journal.RecordData> extends Lifecycle {

  CompletableFuture<AppendResult> append(T batchRecord);

  CompletableFuture<Record<T>> get(long offset, int size);

  CompletableFuture<Record<T>> get(long offset);

  long getStartOffset();

  long getConfirmOffset();

  void delete();

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

  class Record<T extends RecordData> {
    private long offset;
    private int size;
    private boolean blank;
    private T data;

    public T getData() {
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

    public static <T extends RecordData> Builder<T> newBuilder() {
      return new Builder<>();
    }
  }

  class Builder<T extends RecordData> {
    private long offset;
    private int size;
    private boolean blank;
    private T data;

    public Builder<T> setOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder<T> setSize(int size) {
      this.size = size;
      return this;
    }

    public Builder<T> setBlank(boolean blank) {
      this.blank = blank;
      return this;
    }

    public Builder<T> setData(T data) {
      this.data = data;
      return this;
    }

    public Record<T> build() {
      Record<T> record = new Record();
      record.offset = offset;
      record.size = size;
      record.blank = blank;
      record.data = data;
      return record;
    }
  }

  interface RecordData {
    byte[] toByteArray();
  }
}
