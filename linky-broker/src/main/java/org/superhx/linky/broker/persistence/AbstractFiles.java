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
import org.superhx.linky.broker.Utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractFiles implements IFiles {
  public static final long NO_OFFSET = -1;
  private String path;
  private String filePrefix;
  private int fileSize;
  private ChannelFiles.Checkpoint checkpoint;
  private List<IFile> files = new CopyOnWriteArrayList<>();
  private NavigableMap<Long, IFile> startOffsets = new ConcurrentSkipListMap<>();
  private volatile IFile last;
  private AtomicLong writeOffset = new AtomicLong();
  private AtomicLong confirmOffset = new AtomicLong();
  private Function<Integer, ByteBuffer> blankFiller;

  public AbstractFiles(
      String path,
      String filePrefix,
      int fileSize,
      BiFunction<IFile, Long, Long> dataChecker,
      Function<Integer, ByteBuffer> blankFiller) {
    this.path = path;
    this.filePrefix = filePrefix;
    this.fileSize = fileSize;
    this.blankFiller = blankFiller;

    Utils.ensureDirOK(this.path);
    this.checkpoint = ChannelFiles.Checkpoint.load(this.path + "/checkpoint.json");
    File dir = new File(this.path);
    File[] filesInDir = dir.listFiles();
    if (filesInDir == null) {
      throw new LinkyIOException(String.format("%s is not directory", this.path));
    }
    List<File> dataFiles =
        Arrays.asList(filesInDir).stream()
            .filter(f -> f.getName().startsWith(filePrefix))
            .sorted(Comparator.comparing(f -> Long.valueOf(f.getName())))
            .collect(Collectors.toList());
    long lso = this.checkpoint.getSlo();
    for (int i = 0; i < dataFiles.size(); i++) {
      IFile ifile = new ChannelFile(dataFiles.get(i).getPath(), fileSize);
      startOffsets.put(ifile.startOffset(), ifile);
      this.files.add(ifile);
      if (lso >= ifile.startOffset() + ifile.length()) {
        ifile.confirmOffset(ifile.startOffset() + ifile.length());
        ifile.writeOffset(ifile.startOffset() + ifile.length());
        continue;
      }
      while (lso < ifile.startOffset() + ifile.length()) {
        long newLso = dataChecker.apply(ifile, lso);
        if (newLso == lso) {
          break;
        }
        lso = newLso;
      }
    }
    this.writeOffset.set(lso);
    this.confirmOffset.set(lso);
    if (this.files.size() != 0) {
      this.last = this.files.get(this.files.size() - 1);
    }
  }

  protected abstract IFile newFile(String path, int fileSize);

  @Override
  public void init() {
    for (IFile f : files) {
      f.init();
    }
  }

  @Override
  public void start() {
    for (IFile f : files) {
      f.start();
    }
  }

  @Override
  public void shutdown() {
    force();
    for (IFile f : files) {
      f.shutdown();
    }
    checkpoint.persist();
  }

  @Override
  public synchronized AppendResult append(ByteBuffer byteBuffer) {
    for (; ; ) {
      IFile last = this.last;
      if (last == null) {
        this.last =
            newFile(this.path + "/" + filePrefix + "." + Utils.offset2FileName(0), fileSize);
        this.files.add(this.last);
        this.startOffsets.put(this.last.startOffset(), this.last);
        last = this.last;
      }
      int size = byteBuffer.remaining();
      long offset = this.writeOffset.getAndAdd(size);
      if (last.remaining() < size) {
        if (last.remaining() != 0) {
          last.write(blankFiller.apply(last.remaining()), offset);
        }
        last.force();
        this.last =
            newFile(
                this.path + "/" + filePrefix + "." + Utils.offset2FileName(last.writeOffset()),
                fileSize);
        files.add(this.last);
        this.startOffsets.put(this.last.startOffset(), this.last);
        this.writeOffset.set(this.last.startOffset());
        continue;
      }

      last.write(byteBuffer, offset);
      return new AppendResult(offset);
    }
  }

  @Override
  public CompletableFuture<ByteBuffer> read(long offset, int size) {
    IFile file = this.last;
    if (offset < file.startOffset()) {
      file = startOffsets.floorEntry(offset).getValue();
    }
    return CompletableFuture.completedFuture(file.read(offset, size));
  }

  @Override
  public long force() {
    long writeOffset = this.writeOffset.get();
    if (last != null) {
      last.force();
      this.confirmOffset.set(writeOffset);
      this.checkpoint.setSlo(writeOffset);
    }
    return writeOffset;
  }

  @Override
  public long startOffset() {
    Map.Entry<Long, IFile> entry = this.startOffsets.firstEntry();
    if (entry == null) {
      return NO_OFFSET;
    }
    return entry.getKey();
  }

  @Override
  public long writeOffset() {
    return this.writeOffset.get();
  }

  @Override
  public long confirmOffset() {
    return this.confirmOffset.get();
  }

  @Override
  public void delete() {
    checkpoint.delete();
    for (IFile file : files) {
      file.delete();
    }
  }

  @Override
  public IFile getFile(long position) {
    IFile file = this.last;
    if (position < file.startOffset()) {
      file = startOffsets.floorEntry(position).getValue();
    }
    return file;
  }

  @Override
  public void deleteFile(IFile file) {
      if (file == this.last) {
          return;
      }
      checkpoint.persist();
      file.delete();
  }
}
