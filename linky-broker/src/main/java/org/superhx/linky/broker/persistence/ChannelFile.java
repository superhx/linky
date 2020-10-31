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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ChannelFile implements IFile {
  private String file;
  private final long startOffset;
  private long writeOffset;
  private long confirmOffset;
  private long length;
  private RandomAccessFile randomAccessFile;
  private FileChannel channel;
  private int fileSize;

  public ChannelFile(String file, int fileSize) {
    AbstractJournal.ensureDirOK(new File(file).getParent());
    this.file = file;
    this.fileSize = fileSize;
    String fileName = new File(file).getName();
    this.startOffset = Long.valueOf(fileName.substring(fileName.lastIndexOf(".") + 1));
    try {
      randomAccessFile = new RandomAccessFile(file, "rw");
      this.channel = randomAccessFile.getChannel();
      randomAccessFile.setLength(fileSize);
      this.writeOffset = this.startOffset;
      this.confirmOffset = this.startOffset;
      this.length = randomAccessFile.length();
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

  @Override
  public void write(ByteBuffer byteBuffer, long offset) {
    try {
      int size = byteBuffer.limit();
      int writtenBytes = 0;
      for (; ; ) {
        writtenBytes += channel.write(byteBuffer, offset - startOffset + writtenBytes);
        this.writeOffset += size;
        if (writtenBytes != size) {
          continue;
        } else {
          break;
        }
      }
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public void force() {
    try {
      long writeOffset = this.writeOffset;
      channel.force(true);
      this.confirmOffset = writeOffset;
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
  }

  @Override
  public ByteBuffer read(long position, int size) {
    ByteBuffer buf = ByteBuffer.allocateDirect(size);
    position -= startOffset;
    try {
      int totalReadBytes = 0;
      for (; ; ) {
        totalReadBytes += channel.read(buf, position + totalReadBytes);
        if (totalReadBytes != size) {
          continue;
        } else {
          break;
        }
      }
    } catch (IOException e) {
      throw new LinkyIOException(e);
    }
    buf.flip();
    return buf;
  }

  @Override
  public int remaining() {
    return fileSize - (int) (writeOffset - startOffset);
  }

  @Override
  public long length() {
    return this.length;
  }

  @Override
  public long startOffset() {
    return this.startOffset;
  }

  @Override
  public long writeOffset() {
    return this.writeOffset;
  }

  @Override
  public void writeOffset(long writeOffset) {
    this.writeOffset = writeOffset;
  }

  @Override
  public void confirmOffset(long confirmOffset) {
    this.confirmOffset = confirmOffset;
  }

  @Override
  public long confirmOffset() {
    return this.confirmOffset;
  }

  @Override
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
