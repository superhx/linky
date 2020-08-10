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

import org.superhx.linky.broker.LinkyIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;

public class MappedFile {
  private String file;
  private final long startOffset;
  private long writeOffset;
  private long confirmOffset;
  private long length;
  private RandomAccessFile randomAccessFile;
  private FileChannel channel;
  private MappedByteBuffer mappedByteBuffer;
  private int fileSize;

  public MappedFile(String file, int fileSize) {
    LocalWriteAheadLog.ensureDirOK(new File(file).getParent());
    this.file = file;
    this.fileSize = fileSize;
    this.startOffset = Long.valueOf(new File(file).getName());
    boolean readOnly = false;
    try {
      randomAccessFile = new RandomAccessFile(file, readOnly ? "r" : "rw");
      this.channel = randomAccessFile.getChannel();
      if (readOnly) {
        this.mappedByteBuffer =
            this.channel.map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length());
        this.writeOffset = this.startOffset + (int) randomAccessFile.length();
        this.confirmOffset = this.writeOffset;
        this.length = randomAccessFile.length();
      } else {
        randomAccessFile.setLength(fileSize);
        this.mappedByteBuffer = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        this.confirmOffset = this.startOffset;
        this.length = randomAccessFile.length();
      }
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  public CompletableFuture<Void> write(long offset, ByteBuffer byteBuffer) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    try {
      int size = byteBuffer.limit();
      channel.position(offset);
      channel.write(byteBuffer);
      this.writeOffset += size;
      channel.force(false);
      this.confirmOffset += size;
      return result.completedFuture(null);
    } catch (IOException e) {
      result.completeExceptionally(new LinkyIOException(e));
      return result;
    }
  }

  public void sync() {
    try {
      channel.force(true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public CompletableFuture<ByteBuffer> asyncRead(long position, int size) {
    int relativePosition = (int) (position - startOffset);
    ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
    byteBuffer.position(relativePosition);
    byteBuffer.limit(relativePosition + size);
    return CompletableFuture.completedFuture(byteBuffer);
  }

  public ByteBuffer read(long position, int size) {
    try {
      return asyncRead(position, size).get();
    } catch (Exception e) {
      throw new LinkyIOException(e);
    }
  }

  public int remaining() {
    return fileSize - (int) (writeOffset - startOffset);
  }

  public long length() {
    return this.length;
  }

  public long getStartOffset() {
    return this.startOffset;
  }

  public long getWriteOffset() {
    return this.writeOffset;
  }

  public void setWriteOffset(long writeOffset) {
    this.writeOffset = writeOffset;
    try {
      channel.position(this.writeOffset - this.startOffset);
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  public void setConfirmOffset(long confirmOffset) {
    this.confirmOffset = confirmOffset;
  }

  public long getConfirmOffset() {
    return this.confirmOffset;
  }

  @Override
  public String toString() {
    return this.file;
  }
}
