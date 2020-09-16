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
import org.superhx.linky.broker.Utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractJournal<T extends Journal.RecordData> implements Journal {
  public static final int RECORD_MAGIC_CODE = -19951994;
  public static final int BLANK_MAGIC_CODE = -2333;
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private static final int fileSize = 1024 * 1024 * 1024;
  private static final int MAX_WAITING_BYTES = 1024 * 1024 * 4;
  private static final int HEADER_SIZE = 4 + 4;
  private List<AppendHook> appendHookList = new CopyOnWriteArrayList<>();
  private ChannelFiles channelFiles;
  private ConcurrentLinkedQueue<AppendResult> waitingConfirm = new ConcurrentLinkedQueue<>();
  private ScheduledExecutorService forceExecutor =
      Utils.newScheduledThreadPool(1, "LocalWriteAheadLogForce-");
  private AtomicLong waitingConfirmBytes = new AtomicLong();

  public AbstractJournal(String storePath) {
    this.channelFiles =
        new ChannelFiles(
            storePath,
            fileSize,
            (ifile, lso) -> {
              ByteBuffer header = ifile.read(lso, 8);
              int size = header.getInt();
              int magicCode = header.getInt();
              if (size == 0) {
                log.debug("{} scan pos {} return noop", ifile, lso);
              } else if (magicCode == BLANK_MAGIC_CODE) {
                log.debug("{} scan pos {} return blank msg", ifile, lso);
                lso = ifile.startOffset() + ifile.length();
              } else if (magicCode == RECORD_MAGIC_CODE) {
                log.debug("{} scan pos {} return msg", ifile, lso);
                lso += size;
              } else {
                log.debug("{} scan pos {} unknown magic", ifile, lso);
                throw new RuntimeException();
              }
              return lso;
            },
            size -> {
              ByteBuffer buf = ByteBuffer.allocate(size);
              buf.putInt(BLANK_MAGIC_CODE);
              buf.putInt(size - 8);
              buf.put(new byte[size - 8]);
              buf.flip();
              return buf;
            });
  }

  @Override
  public void init() {
    channelFiles.init();
  }

  @Override
  public void start() {
    channelFiles.start();
    forceExecutor.scheduleAtFixedRate(() -> force(), 1, 1, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    channelFiles.shutdown();
    forceExecutor.shutdown();
  }

  @Override
  public CompletableFuture<AppendResult> append(RecordData recordData) {
    byte[] data = recordData.toByteArray();
    int size = 4 + 4 + data.length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    byteBuffer.putInt(size);
    byteBuffer.putInt(RECORD_MAGIC_CODE);
    byteBuffer.put(data);
    byteBuffer.flip();
    AppendResult appendResult;
    synchronized (this) {
      IFiles.AppendResult mappedFileAppendResult = channelFiles.append(byteBuffer);
      appendResult = new AppendResult(mappedFileAppendResult.getOffset(), size);
      appendResult.thenAccept(
          r ->
              afterAppend(
                  Record.newBuilder()
                      .setOffset(r.getOffset())
                      .setSize(r.getSize())
                      .setData(recordData)
                      .build()));
      waitingConfirm.add(appendResult);
      if (waitingConfirmBytes.addAndGet(size) > MAX_WAITING_BYTES) {
        force();
      }
    }
    return appendResult;
  }

  public void force() {
    forceExecutor.submit(() -> force0());
  }

  protected void force0() {
    long lastWaitingConfirmBytes = this.waitingConfirmBytes.get();
    long confirmPhysicalOffset = channelFiles.force();
    this.waitingConfirmBytes.getAndAdd(-lastWaitingConfirmBytes);
    for (; ; ) {
      AppendResult rst = waitingConfirm.peek();
      if (rst == null) {
        break;
      }
      if (rst.getOffset() >= confirmPhysicalOffset) {
        break;
      }
      waitingConfirm.poll();
      rst.complete(rst);
    }
  }

  @Override
  public CompletableFuture<Record<T>> get(long offset, int size) {
    return channelFiles.read(offset, size).thenApply(b -> parse(offset, b));
  }

  @Override
  public CompletableFuture<Record<T>> get(long offset) {
    AtomicInteger size = new AtomicInteger();
    return channelFiles
        .read(offset, HEADER_SIZE)
        .thenCompose(
            header -> {
              size.set(header.getInt());
              return channelFiles.read(offset, size.get());
            })
        .thenApply(buf -> parse(offset, buf));
  }

  @Override
  public void registerAppendHook(AppendHook hook) {
    appendHookList.add(hook);
  }

  @Override
  public long getStartOffset() {
    return channelFiles.startOffset();
  }

  @Override
  public long getConfirmOffset() {
    return channelFiles.confirmOffset();
  }

  @Override
  public void delete() {
    channelFiles.delete();
  }

  protected abstract Record<T> parse(long offset, ByteBuffer byteBuffer);

  protected void afterAppend(Record record) {
    for (AppendHook appendHook : appendHookList) {
      appendHook.afterAppend(record);
    }
  }

  public static void ensureDirOK(final String dirName) {
    if (dirName != null) {
      File f = new File(dirName);
      if (!f.exists()) {
        boolean result = f.mkdirs();
        log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
      }
    }
  }
}
