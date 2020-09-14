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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
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

public class MappedFiles implements Lifecycle {
  public static final long NO_OFFSET = -1;
  private static final Logger log = LoggerFactory.getLogger(MappedFiles.class);
  private String path;
  private int fileSize;
  private Checkpoint checkpoint;
  private List<MappedFile> files = new CopyOnWriteArrayList<>();
  private NavigableMap<Long, MappedFile> startOffsets = new ConcurrentSkipListMap<>();
  private volatile MappedFile last;
  private AtomicLong writeOffset = new AtomicLong();
  private AtomicLong confirmOffset = new AtomicLong();
  private Function<Integer, ByteBuffer> blankFiller;

  public MappedFiles(
      String path,
      int fileSize,
      BiFunction<MappedFile, Long, Long> dataChecker,
      Function<Integer, ByteBuffer> blankFiller) {
    this.path = path;
    this.fileSize = fileSize;
    this.blankFiller = blankFiller;

    Utils.ensureDirOK(this.path);
    this.checkpoint = Checkpoint.load(this.path + "/checkpoint.json");
    File dir = new File(this.path);
    File[] filesInDir = dir.listFiles();
    if (filesInDir == null) {
      throw new LinkyIOException(String.format("%s is not directory", this.path));
    }
    List<File> dataFiles =
        Arrays.asList(filesInDir).stream()
            .filter(f -> !f.getName().contains("json"))
            .sorted(Comparator.comparing(f -> Integer.valueOf(f.getName())))
            .collect(Collectors.toList());
    long lso = this.checkpoint.getSlo();
    for (int i = 0; i < dataFiles.size(); i++) {
      File file = dataFiles.get(i);
      MappedFile mappedFile = new MappedFile(file.getPath(), fileSize);
      startOffsets.put(mappedFile.getStartOffset(), mappedFile);
      this.files.add(mappedFile);
      if (lso >= mappedFile.getStartOffset() + mappedFile.length()) {
        mappedFile.setConfirmOffset(mappedFile.getStartOffset() + mappedFile.length());
        mappedFile.setWriteOffset(mappedFile.getStartOffset() + mappedFile.length());
        continue;
      }
      while (lso < mappedFile.getStartOffset() + mappedFile.length()) {
        long newLso = dataChecker.apply(mappedFile, lso);
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

  @Override
  public void init() {
    for (MappedFile mappedFile : files) {
      mappedFile.init();
    }
  }

  @Override
  public void start() {
    for (MappedFile mappedFile : files) {
      mappedFile.start();
    }
  }

  @Override
  public void shutdown() {
    force();
    for (MappedFile mappedFile : files) {
      mappedFile.shutdown();
    }
    checkpoint.persist();
  }

  public synchronized AppendResult append(ByteBuffer byteBuffer) {
    for (; ; ) {
      MappedFile last = this.last;
      if (last == null) {
        this.last = new MappedFile(this.path + "/" + Utils.offset2FileName(0), fileSize);
        this.startOffsets.put(this.last.getStartOffset(), this.last);
        last = this.last;
      }
      int size = byteBuffer.remaining();
      long offset = this.writeOffset.getAndAdd(size);
      if (last.remaining() < size) {
        if (last.remaining() != 0) {
          last.write(offset, blankFiller.apply(last.remaining()));
        }
        last.force();
        this.last =
            new MappedFile(
                this.path + "/" + Utils.offset2FileName(last.getWriteOffset()), fileSize);
        this.startOffsets.put(this.last.getStartOffset(), this.last);
        this.writeOffset.set(this.last.getStartOffset());
        continue;
      }

      last.write(offset, byteBuffer);
      return new AppendResult(offset);
    }
  }

  public CompletableFuture<ByteBuffer> read(long offset, int size) {
    MappedFile file = this.last;
    if (offset < file.getStartOffset()) {
      file = startOffsets.floorEntry(offset).getValue();
    }
    return CompletableFuture.completedFuture(file.read(offset, size));
  }

  public long force() {
    long writeOffset = this.writeOffset.get();
    if (last != null) {
      last.force();
      this.confirmOffset.set(writeOffset);
      this.checkpoint.setSlo(writeOffset);
    }
    return writeOffset;
  }

  public long getStartOffset() {
    Map.Entry<Long, MappedFile> entry = this.startOffsets.firstEntry();
    if (entry == null) {
      return NO_OFFSET;
    }
    return entry.getKey();
  }

  public long getConfirmOffset() {
    return this.confirmOffset.get();
  }

  public long getWriteOffset() {
    return this.writeOffset.get();
  }

  static class Checkpoint {
    private String path;
    private long slo;

    public Checkpoint() {}

    public static Checkpoint load(String path) {
      Checkpoint checkpoint = Utils.file2jsonObj(path, Checkpoint.class);
      if (checkpoint == null) {
        checkpoint = new Checkpoint();
      }
      checkpoint.path = path;
      return checkpoint;
    }

    public long getSlo() {
      return slo;
    }

    public void setSlo(long slo) {
      this.slo = slo;
    }

    public void persist() {
      Utils.jsonObj2file(this, path);
    }
  }

  static class AppendResult {
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
