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

import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public class MappedFile implements Lifecycle {
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
        this.writeOffset = this.startOffset;
        this.confirmOffset = this.startOffset;
        this.length = randomAccessFile.length();
      }
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public void shutdown() {
    force();
    try {
      if (this.channel != null) {
        this.channel.close();
      }
      if (this.randomAccessFile != null) {
        this.randomAccessFile.close();
      }
    } catch (IOException e) {
    }
  }

  public void write(long offset, ByteBuffer byteBuffer) {
    try {
      int size = byteBuffer.limit();
      channel.position(offset - startOffset);
      channel.write(byteBuffer);
      this.writeOffset += size;
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  public void force() {
    try {
      long writeOffset = this.writeOffset;
      channel.force(true);
      this.confirmOffset = writeOffset;
    } catch (IOException e) {
      throw new LinkyIOException(e);
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

  public void delete() {
    try {
      Files.delete(Paths.get(file));
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public String toString() {
    return this.file;
  }
}
