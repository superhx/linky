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

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.service.proto.BatchRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LocalWriteAheadLog implements WriteAheadLog {
  public static final int RECORD_MAGIC_CODE = -19951994;
  public static final int BLANK_MAGIC_CODE = -2333;
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private static final int fileSize = 1024 * 1024 * 1024;
  private static final long MAX_WAITING_BYTES = 1024 * 1024;
  private static final int HEADER_SIZE = 4 + 4;
  private List<AppendHook> appendHookList = new CopyOnWriteArrayList<>();
  private MappedFiles mappedFiles;
  private ConcurrentLinkedQueue<AppendResult> waitingConfirm = new ConcurrentLinkedQueue<>();
  private ScheduledExecutorService forceExecutor =
      Utils.newScheduledThreadPool(1, "LocalWriteAheadLogForce-");
  private AtomicLong waitingConfirmBytes = new AtomicLong();
  private BlockingQueue<ByteBuffer> waitingBuffers = new ArrayBlockingQueue<ByteBuffer>(1024);

  public LocalWriteAheadLog(String storePath) {
    this.mappedFiles =
        new MappedFiles(
            storePath,
            fileSize,
            (mappedFile, lso) -> {
              ByteBuffer header = mappedFile.read(lso, 8);
              int size = header.getInt();
              int magicCode = header.getInt();
              if (size == 0) {
                log.debug("{} scan pos {} return noop", mappedFile, lso);
              } else if (magicCode == BLANK_MAGIC_CODE) {
                log.debug("{} scan pos {} return blank msg", mappedFile, lso);
                lso = mappedFile.getStartOffset() + mappedFile.length();
              } else if (magicCode == RECORD_MAGIC_CODE) {
                log.debug("{} scan pos {} return msg", mappedFile, lso);
                lso += size;
              } else {
                log.debug("{} scan pos {} unknown magic", mappedFile, lso);
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
    mappedFiles.init();
  }

  @Override
  public void start() {
    mappedFiles.start();
    forceExecutor.scheduleAtFixedRate(() -> force(), 1, 1, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    mappedFiles.shutdown();
    forceExecutor.shutdown();
  }

  @Override
  public AppendResult append(BatchRecord batchRecord) {
    byte[] data = batchRecord.toByteArray();
    int size = 4 + 4 + data.length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    byteBuffer.putInt(size);
    byteBuffer.putInt(RECORD_MAGIC_CODE);
    byteBuffer.put(data);
    byteBuffer.flip();
    AppendResult appendResult;
    synchronized (this) {
      appendResult = append0(byteBuffer);
      appendResult.thenAccept(r -> afterAppend(batchRecord, appendResult));
    }
    return appendResult;
  }

  public AppendResult append0(ByteBuffer byteBuffer) {
    int size = byteBuffer.limit();
    MappedFiles.AppendResult mappedFileAppendResult = mappedFiles.append(byteBuffer);
    AppendResult appendResult = new AppendResult(mappedFileAppendResult.getOffset(), size);
    waitingConfirm.add(appendResult);
    if (waitingConfirmBytes.addAndGet(size) > MAX_WAITING_BYTES) {
      force();
    }
    return appendResult;
  }

  public void force() {
    forceExecutor.submit(() -> force0());
  }

  protected void force0() {
    long lastWaitingConfirmBytes = this.waitingConfirmBytes.get();
    long confirmPhysicalOffset = mappedFiles.force();
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
      rst.complete();
    }
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset, int size) {
    return mappedFiles.read(offset, size).thenApply(b -> parse(b));
  }

  @Override
  public CompletableFuture<WalBatchRecord> get(long offset) {
    AtomicInteger size = new AtomicInteger();
    return mappedFiles
        .read(offset, HEADER_SIZE)
        .thenCompose(
            header -> {
              size.set(header.getInt());
              return mappedFiles.read(offset, size.get());
            })
        .thenApply(buf -> new WalBatchRecord(parse(buf), size.get()));
  }

  @Override
  public void registerAppendHook(AppendHook hook) {
    appendHookList.add(hook);
  }

  @Override
  public long getStartOffset() {
    return mappedFiles.getStartOffset();
  }

  @Override
  public long getConfirmOffset() {
    return mappedFiles.getConfirmOffset();
  }

  @Override
  public void delete() {
    mappedFiles.delete();
  }

  protected BatchRecord parse(ByteBuffer byteBuffer) {
    ByteBuffer data = byteBuffer.slice();
    data.getInt();
    data.getInt();
    try {
      return BatchRecord.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    return null;
  }

  protected void afterAppend(BatchRecord batchRecord, AppendResult appendResult) {
    for (AppendHook appendHook : appendHookList) {
      appendHook.afterAppend(batchRecord, appendResult);
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
