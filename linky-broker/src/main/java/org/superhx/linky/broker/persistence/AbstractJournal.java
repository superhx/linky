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
import org.superhx.linky.broker.Configuration;
import org.superhx.linky.broker.Utils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public abstract class AbstractJournal<T extends Journal.RecordData> implements Journal {
  public static final int RECORD_MAGIC_CODE = -19951994;
  public static final int BLANK_MAGIC_CODE = -2333;
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private static final int fileSize = 1024 * 1024 * 1024;
  private static final int HEADER_SIZE = 4 + 4;

  public static final String MAX_WAITING_APPEND_RECORD_COUNT_KEY =
      "MAX_WAITING_APPEND_RECORD_COUNT";
  private static final int DEFAULT_MAX_WAITING_APPEND_RECORD_COUNT = 4096;

  public static final String MAX_WAITING_APPEND_BYTES_KEY = "MAX_WAITING_APPEND_BYTES";
  private static final int DEFAULT_MAX_WAITING_APPEND_BYTES = 1024 * 1024 * 32;

  public static final String GROUP_APPEND_BATCH_SIZE_KEY = "GROUP_APPEND_BATCH_SIZE";
  private static final int DEFAULT_GROUP_APPEND_BATCH_SIZE = 1024 * 1024 * 16;

  public static final String GROUP_APPEND_INTERVAL_KEY = "GROUP_APPEND_INTERVAL";
  private static final long DEFAULT_GROUP_APPEND_INTERVAL = 100;

  public static final String MAX_WAITING_FORCE_BYTES_KEY = "DEFAULT_MAX_WAITING_FORCE_BYTES";
  private static final int DEFAULT_MAX_WAITING_FORCE_BYTES = 1024 * 1024 * 32;

  public static final String FORCE_INTERVAL_KEY = "FORCE_INTERVAL";
  private static final long DEFAULT_FORCE_INTERVAL = 1000;

  private String path;
  private ChannelFiles iFiles;

  private final int maxWaitingAppendBytes;
  private final AtomicLong waitingAppendBytes = new AtomicLong();
  private final int groupAppendBatchSize;
  private final ByteBuffer groupAppendBuffer;
  private final BlockingQueue<WaitingAppend> waitingGroupAppend;
  private final long groupAppendInterval;
  private final ScheduledExecutorService groupAppendExecutor =
      Utils.newScheduledThreadPool(1, "LocalWriteAheadLogGroupAppend-");

  private final int maxWaitingForceBytes;
  private final AtomicLong waitingForceBytes = new AtomicLong();
  private final ConcurrentLinkedQueue<WaitingForce> waitingConfirm = new ConcurrentLinkedQueue<>();
  private final long forceInterval;
  private final ScheduledExecutorService forceExecutor =
      Utils.newScheduledThreadPool(1, "LocalWriteAheadLogForce-");

  public AbstractJournal(String storePath, Configuration config) {
    this.path = storePath;
    this.iFiles =
        new ChannelFiles(
            path,
            "linkylog",
            fileSize,
            (ifile, lso) -> {
              ByteBuffer header = ifile.read(lso, 8);
              int size = header.getInt();
              int magicCode = header.getInt();
              if (size == 0) {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return noop", ifile, lso);
                }
              } else if (magicCode == BLANK_MAGIC_CODE) {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return blank msg", ifile, lso);
                }
                lso = ifile.startOffset() + ifile.length();
              } else if (magicCode == RECORD_MAGIC_CODE) {
                if (log.isDebugEnabled()) {
                  log.debug("{} scan pos {} return msg", ifile, lso);
                }
                lso += size;
              } else {
                log.error("{} scan pos {} unknown magic", ifile, lso);
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

    maxWaitingAppendBytes =
        config.getInt(MAX_WAITING_APPEND_BYTES_KEY, DEFAULT_MAX_WAITING_APPEND_BYTES);
    groupAppendBatchSize =
        config.getInt(GROUP_APPEND_BATCH_SIZE_KEY, DEFAULT_GROUP_APPEND_BATCH_SIZE);
    groupAppendBuffer = ByteBuffer.allocateDirect(groupAppendBatchSize);
    waitingGroupAppend =
        new ArrayBlockingQueue<>(
            config.getInt(
                MAX_WAITING_APPEND_RECORD_COUNT_KEY, DEFAULT_MAX_WAITING_APPEND_RECORD_COUNT));
    groupAppendInterval = config.getLong(GROUP_APPEND_INTERVAL_KEY, DEFAULT_GROUP_APPEND_INTERVAL);

    maxWaitingForceBytes =
        config.getInt(MAX_WAITING_FORCE_BYTES_KEY, DEFAULT_MAX_WAITING_FORCE_BYTES);
    forceInterval = config.getLong(FORCE_INTERVAL_KEY, DEFAULT_FORCE_INTERVAL);
  }

  @Override
  public void init() {
    iFiles.init();
  }

  @Override
  public void start() {
    iFiles.start();
    forceExecutor.scheduleAtFixedRate(
        () -> force0(true), forceInterval, forceInterval, TimeUnit.MICROSECONDS);
    groupAppendExecutor.scheduleWithFixedDelay(
        () -> doAppend(null, true),
        groupAppendInterval,
        groupAppendInterval,
        TimeUnit.MICROSECONDS);
  }

  @Override
  public void shutdown() {
    doAppend(null, true);
    force0(true);
    iFiles.shutdown();
    groupAppendExecutor.shutdown();
    forceExecutor.shutdown();
  }

  @Override
  public String getPath() {
    return this.path;
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
    WaitingAppend waitingAppend = new WaitingAppend(byteBuffer, size);
    Consumer<AppendResult> hook = getHook(recordData);
    waitingAppend
        .future()
        .thenApply(
            r -> {
              hook.accept(r);
              return r;
            });
    if (waitingGroupAppend.offer(waitingAppend)) {
      if (waitingAppendBytes.addAndGet(size) > maxWaitingAppendBytes) {
        groupAppendExecutor.submit(() -> doAppend(null, false));
      }
    } else {
      doAppend(waitingAppend, false);
    }
    return waitingAppend.future();
  }

  public synchronized void doAppend(WaitingAppend oneMore, boolean force) {
    if (!force) {
      if (oneMore != null) {
        if (waitingGroupAppend.offer(oneMore)) {
          if (waitingAppendBytes.addAndGet(oneMore.size()) < maxWaitingAppendBytes) {
            return;
          }
          doAppend(null, false);
          return;
        }
      } else {
        if (waitingAppendBytes.get() < maxWaitingAppendBytes) {
          return;
        }
      }
    }
    List<WaitingAppend> waitingAppends = new ArrayList<>(waitingGroupAppend.size() + 1);
    waitingGroupAppend.drainTo(waitingAppends);
    if (oneMore != null) {
      waitingAppends.add(oneMore);
    }
    long relatedOffset = 0;
    List<WaitingAppend> group = new ArrayList<>(waitingAppends.size());
    for (int i = 0; i < waitingAppends.size(); i++) {
      WaitingAppend waitingAppend = waitingAppends.get(i);
      int size = waitingAppend.size();
      if ((i == waitingAppends.size() - 1)
          || (size > groupAppendBuffer.remaining() && groupAppendBuffer.position() != 0)) {
        if (groupAppendBuffer.position() == 0) {
          continue;
        }
        groupAppendBuffer.flip();
        long offset = iFiles.append(groupAppendBuffer).getOffset();
        groupAppendBuffer.clear();
        for (WaitingAppend w : group) {
          this.waitingConfirm.add(
              new WaitingForce(w.future(), new AppendResult(offset + w.relatedOffset(), w.size())));
        }
        waitingAppendBytes.addAndGet(-relatedOffset);
        if (waitingForceBytes.addAndGet(relatedOffset) > maxWaitingForceBytes) {
          forceExecutor.submit(() -> force0(false));
        }
        relatedOffset = 0;
        group.clear();
        if (i == waitingAppends.size() - 1) {
          return;
        } else {
          i--;
          continue;
        }
      }

      if (size > groupAppendBatchSize) {
        long offset = iFiles.append(waitingAppend.buf()).getOffset();
        this.waitingConfirm.add(
            new WaitingForce(waitingAppend.future(), new AppendResult(offset, size)));
        waitingAppendBytes.addAndGet(-size);
        if (waitingForceBytes.addAndGet(size) > maxWaitingForceBytes) {
          forceExecutor.submit(() -> force0(false));
        }
        continue;
      }

      groupAppendBuffer.put(waitingAppend.buf());
      group.add(waitingAppend);
      waitingAppend.relatedOffset(relatedOffset);
      relatedOffset += size;
    }
  }

  protected Consumer<AppendResult> getHook(RecordData recordData) {
    return r -> {};
  }

  protected void force0(boolean force) {
    if (!force && waitingAppendBytes.get() < maxWaitingForceBytes) {
      return;
    }
    long lastWaitingConfirmBytes = this.waitingForceBytes.get();
    long confirmPhysicalOffset = iFiles.force();
    this.waitingForceBytes.getAndAdd(-lastWaitingConfirmBytes);
    for (; ; ) {
      WaitingForce waitingForce = waitingConfirm.peek();
      if (waitingForce == null) {
        break;
      }
      if (waitingForce.appendResult().getOffset() >= confirmPhysicalOffset) {
        break;
      }
      waitingConfirm.poll();
      waitingForce.complete();
    }
  }

  @Override
  public CompletableFuture<Record<T>> get(long offset, int size) {
    return iFiles.read(offset, size).thenApply(b -> parse(offset, b));
  }

  @Override
  public CompletableFuture<Record<T>> get(long offset) {
    AtomicInteger size = new AtomicInteger();
    return iFiles
        .read(offset, HEADER_SIZE)
        .thenCompose(
            header -> {
              size.set(header.getInt());
              return iFiles.read(offset, size.get());
            })
        .thenApply(buf -> parse(offset, buf));
  }

  @Override
  public long getStartOffset() {
    return iFiles.startOffset();
  }

  @Override
  public long getConfirmOffset() {
    return iFiles.confirmOffset();
  }

  @Override
  public void delete() {
    iFiles.delete();
  }

  protected abstract Record<T> parse(long offset, ByteBuffer byteBuffer);

  public static void ensureDirOK(final String dirName) {
    if (dirName != null) {
      File f = new File(dirName);
      if (!f.exists()) {
        boolean result = f.mkdirs();
        log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
      }
    }
  }

  static class WaitingAppend {
    private ByteBuffer buf;
    private long relatedOffset;
    private int size;
    private CompletableFuture<AppendResult> future = new CompletableFuture<>();

    public WaitingAppend(ByteBuffer buf, int size) {
      this.buf = buf;
      this.size = size;
    }

    public ByteBuffer buf() {
      return buf;
    }

    public WaitingAppend relatedOffset(long offset) {
      this.relatedOffset = offset;
      return this;
    }

    public long relatedOffset() {
      return relatedOffset;
    }

    public int size() {
      return size;
    }

    public CompletableFuture<AppendResult> future() {
      return future;
    }
  }

  static class WaitingForce {
    private CompletableFuture<AppendResult> future;
    private AppendResult appendResult;

    public WaitingForce(CompletableFuture<AppendResult> future, AppendResult appendResult) {
      this.future = future;
      this.appendResult = appendResult;
    }

    public AppendResult appendResult() {
      return appendResult;
    }

    public void complete() {
      this.future.complete(appendResult);
    }
  }
}
