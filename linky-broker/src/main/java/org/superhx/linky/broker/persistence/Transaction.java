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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;
import org.superhx.linky.broker.persistence.TransactionIndex.Confirm;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.Record;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.stream.Collectors;

import static org.superhx.linky.broker.persistence.Constants.TRANS_LSO_KEY;
import static org.superhx.linky.broker.persistence.Partition.AppendContext.TRANS_CONFIRM_CTX_KEY;
import static org.superhx.linky.broker.persistence.Partition.AppendContext.TRANS_MSG_CTX_KEY;

public class Transaction implements Lifecycle {
  private static final Logger log = LoggerFactory.getLogger(Transaction.class);
  private static final ExecutorService CATCHUP_EXECUTOR =
      Utils.newFixedThreadPool(1, "TRANSACTION_CATCHUP");
  private static final ScheduledExecutorService TRANSACTION_INDEX_EXECUTOR =
      Utils.newScheduledThreadPool(1, "TRANSACTION_INDEX_EXECUTOR");

  private AtomicReference<Status> status = new AtomicReference<>(Status.INIT);
  private volatile Cursor checkpoint;
  private volatile Cursor nextOffset;
  private volatile Cursor nextBuildCursor;
  private Map<Integer, TransactionCursor> unknownCursors = new ConcurrentHashMap<>();
  private Map<Integer, TransactionCursor> rollbackCursors = new ConcurrentHashMap<>();
  private Build build = new Build();
  private Catchup catchup = new Catchup();
  private volatile Cursor expectedCatchup;

  private Queue<TransactionIndex> transactionIndexQueue = new LinkedBlockingQueue<>();

  private Partition partition;

  public Transaction(Partition partition) {
    this.partition = partition;
    partition.registerAppendHook(getAppendHook());
  }

  @Override
  public void init() {
    byte[] transLSOBytes = partition.getMeta(TRANS_LSO_KEY);
    if (transLSOBytes == null) {
      enableTransaction();
      return;
    }
    checkpoint = Cursor.get(transLSOBytes);
    nextOffset = checkpoint;
    nextBuildCursor = checkpoint;
    log.info("[TRANS_META_LOAD]checkpoint={}", checkpoint);
    status.set(Status.START);
    expectedCatchup = partition.getNextCursor();
    catchup.catchup();
  }

  @Override
  public void shutdown() {
    try {
      flushMeta().get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private CompletableFuture<Void> flushMeta() {
    log.info("[TRANS_META_SAVE]{},transactionLSO={}", checkpoint);
    byte[] checkpointBytes = checkpoint.toBytes();
    List<CompletableFuture<Void>> cursorSaveTasks = new LinkedList<>();
    cursorSaveTasks.addAll(
        unknownCursors.values().stream().map(c -> c.save()).collect(Collectors.toList()));
    cursorSaveTasks.addAll(
        rollbackCursors.values().stream().map(c -> c.save()).collect(Collectors.toList()));
    return CompletableFuture.allOf(cursorSaveTasks.toArray(new CompletableFuture[0]))
        .thenCompose(nil -> partition.setMeta(TRANS_LSO_KEY, checkpointBytes));
  }

  private void enableTransaction() {
    log.info("[ENABLE_TRANS]{}", partition.name());
    try {
      Cursor next = partition.getNextCursor();
      checkpoint = next;
      nextOffset = next;
      nextBuildCursor = next;
      partition
          .setMeta(TRANS_LSO_KEY, nextOffset.toBytes())
          .thenAccept(nil -> status.set(Status.START))
          .get();
      expectedCatchup = next;
    } catch (Throwable ex) {
      throw new LinkyIOException(ex);
    }
  }

  public TransactionStatus getTransactionStatus(Cursor cursor) {
    if (cursor.compareTo(nextOffset) >= 0) {
      return TransactionStatus.Unknown;
    }
    TransactionCursor unknownCursor = getUnknownCursor(cursor.getIndex());
    if (!unknownCursor.isLoad()) {
      return TransactionStatus.Unknown;
    }
    if (unknownCursor.contains(cursor.getOffset())) {
      return TransactionStatus.Unknown;
    }
    TransactionCursor rollbackCursor = getRollbackCursor(cursor.getIndex());
    if (!rollbackCursor.isLoad()) {
      return TransactionStatus.Unknown;
    }
    if (rollbackCursor.contains(cursor.getOffset())) {
      return TransactionStatus.Rollback;
    }
    return TransactionStatus.Commit;
  }

  private TransactionCursor getUnknownCursor(int index) {
    TransactionCursor unknownCursor = unknownCursors.get(index);
    if (unknownCursor == null) {
      unknownCursor = new TransactionCursor(index, Constants.TRANS_UNKNOWN_CURSOR_PREFIX);
      unknownCursors.put(index, unknownCursor);
      unknownCursor.load();
    }
    return unknownCursor;
  }

  private TransactionCursor getRollbackCursor(int index) {
    TransactionCursor rollbackCursor = rollbackCursors.get(index);
    if (rollbackCursor == null) {
      rollbackCursor = new TransactionCursor(index, Constants.TRANS_ROLLBACK_CURSOR_PREFIX);
      rollbackCursors.put(index, rollbackCursor);
      rollbackCursor.load();
    }
    return rollbackCursor;
  }

  private boolean unknown(TransactionIndex.Trans trans) {
    Cursor cursor = trans.getCursor();
    TransactionCursor unknownCursor = getUnknownCursor(cursor.getIndex());
    if (!unknownCursor.isLoad()) {
      return false;
    }

    unknownCursor.add(cursor.getOffset());
    nextBuildCursor = cursor.next();
    return true;
  }

  private boolean confirm(TransactionIndex.Confirm confirm) {
    for (Cursor cursor : confirm.getConfirmCursors()) {
      int index = cursor.getIndex();
      if (!confirm.isCommit()) {
        TransactionCursor rollbackCursor = getRollbackCursor(index);
        if (!rollbackCursor.isLoad()) {
          return false;
        }
        rollbackCursor.add(cursor.getOffset());
      }
      TransactionCursor unknownCursor = getUnknownCursor(index);
      if (!unknownCursor.isLoad()) {
        return false;
      }
      unknownCursor.remove(cursor.getOffset());
    }
    nextBuildCursor = confirm.getCursor().next();
    return true;
  }

  private boolean normal(TransactionIndex.Normal normal) {
    nextBuildCursor = normal.getCursor().next();
    return true;
  }

  private void handle(TransactionIndex transactionIndex) {
    expectedCatchup = transactionIndex.getCursor().next();
    if (!transactionIndex.getCursor().equals(nextOffset)) {
      catchup.catchup();
      return;
    }
    transactionIndexQueue.add(transactionIndex);
    nextOffset = transactionIndex.getCursor().next();
    build.build();
  }

  private Partition.AppendHook getAppendHook() {
    return new AppendHook();
  }

  class AppendHook implements Partition.AppendHook {

    @Override
    public void before(Partition.AppendContext ctx, BatchRecord.Builder batchRecord) {
      int flag = batchRecord.getFlag();
      if (Flag.isTransMsg(flag)) {
        ctx.putContext(TRANS_MSG_CTX_KEY, Boolean.TRUE.toString());
      } else if (Flag.isTransConfirm(flag)) {
        Record record = batchRecord.getRecords(0);
        Confirm confirm = Confirm.getConfirm(record.getValue().asReadOnlyByteBuffer());
        ctx.putContext(TRANS_CONFIRM_CTX_KEY, confirm);
      }
    }

    @Override
    public void after(Partition.AppendContext ctx) {
      Object transMsgMark = ctx.getContext(TRANS_MSG_CTX_KEY);
      Object transConfirm = ctx.getContext(TRANS_CONFIRM_CTX_KEY);
      if (transMsgMark != null) {
        handle(TransactionIndex.trans(ctx.getCursor()));
      } else if (transConfirm != null) {
        Confirm confirm = (Confirm) transConfirm;
        confirm.setCursor(ctx.getCursor());
        handle(confirm);
      } else {
        handle(TransactionIndex.normal(ctx.getCursor()));
      }
    }
  }

  class Build implements Runnable {

    private AtomicBoolean running = new AtomicBoolean();
    private LongAccumulator version = new LongAccumulator(Long::sum, 0L);

    @Override
    public void run() {
      long versionSnapshot = version.get();
      for (; ; ) {
        TransactionIndex transactionIndex = transactionIndexQueue.peek();
        if (transactionIndex == null) {
          break;
        }
        boolean success;
        if (transactionIndex instanceof TransactionIndex.Normal) {
          success = normal((TransactionIndex.Normal) transactionIndex);
        } else if (transactionIndex instanceof TransactionIndex.Trans) {
          success = unknown((TransactionIndex.Trans) transactionIndex);
        } else {
          success = confirm((Confirm) transactionIndex);
        }
        if (success) {
          transactionIndexQueue.poll();
        } else {
          break;
        }
      }
      running.set(false);
      if (versionSnapshot != version.get()) {
        build();
      }
    }

    public void build() {
      version.accumulate(1);
      if (running.compareAndSet(false, true)) {
        TRANSACTION_INDEX_EXECUTOR.submit(this);
      }
    }
  }

  class Catchup implements Runnable {
    private AtomicBoolean running = new AtomicBoolean();

    @Override
    public void run() {
      Cursor closedStart = nextBuildCursor;
      Cursor openEnd = expectedCatchup;
      log.info("[CATCHUP_BUILD_TXN_CUROSR]{},from={},to={}", partition.name(), closedStart, openEnd);
      partition.get(
          closedStart.toBytes(),
          openEnd.toBytes(),
          new StreamObserver<BatchRecord>() {
            @Override
            public void onNext(BatchRecord batchRecord) {
              int flag = batchRecord.getFlag();
              TransactionIndex transactionIndex;
              Cursor cursor = new Cursor(batchRecord.getIndex(), batchRecord.getFirstOffset());
              if (Flag.isTransMsg(flag)) {
                transactionIndex = TransactionIndex.trans(cursor);
              } else if (Flag.isTransConfirm(flag)) {
                Record record = batchRecord.getRecords(0);
                Confirm confirm = Confirm.getConfirm(record.getValue().asReadOnlyByteBuffer());
                confirm.setCursor(cursor);
                transactionIndex = confirm;
              } else {
                transactionIndex = TransactionIndex.normal(cursor);
              }
              transactionIndexQueue.offer(transactionIndex);
              build.build();
            }

            @Override
            public void onError(Throwable throwable) {
              throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
              nextOffset = openEnd;
              running.set(false);
              if (!expectedCatchup.equals(openEnd)) {
                catchup();
              }
            }
          });
    }

    public void catchup() {
      if (running.compareAndSet(false, true)) {
        CATCHUP_EXECUTOR.submit(this);
      }
    }
  }

  class TransactionCursor {
    private static final byte LEFT_CLOSE_RIGHT_CLOSE = 0;
    private static final byte LEFT_CLOSE_RIGHT_OPEN = 1;
    private static final byte LEFT_OPEN_RIGHT_CLOSE = 2;
    private static final byte LEFT_OPEN_RIGHT_OPEN = 3;
    private int index;
    private RangeSet<Long> rangeSet = TreeRangeSet.create();
    private byte[] metaKeyPrefix;

    private boolean load;

    public TransactionCursor(int index, byte[] metaKeyPrefix) {
      this.index = index;
      this.metaKeyPrefix = metaKeyPrefix;
    }

    public boolean contains(Long offset) {
      return rangeSet.contains(offset);
    }

    public void add(Long offset) {
      rangeSet.add(Range.singleton(offset));
    }

    public void remove(Long offset) {
      rangeSet.remove(Range.singleton(offset));
    }

    public int getIndex() {
      return index;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public boolean isLoad() {
      return load;
    }

    public void load() {
      byte[] cursorBytes =
          partition.getMeta(ByteBuffer.allocate(8).put(metaKeyPrefix).putInt(index).array());
      if (cursorBytes == null) {
        load = true;
        return;
      }
      ByteBuffer buf = ByteBuffer.wrap(cursorBytes);
      while (buf.hasRemaining()) {
        byte rangeType = buf.get();
        Long left = buf.getLong();
        Long right = buf.getLong();
        switch (rangeType) {
          case LEFT_CLOSE_RIGHT_CLOSE:
            rangeSet.add(Range.closed(left, right));
            break;
          case LEFT_CLOSE_RIGHT_OPEN:
            rangeSet.add(Range.closedOpen(left, right));
            break;
          case LEFT_OPEN_RIGHT_CLOSE:
            rangeSet.add(Range.openClosed(left, right));
            break;
          case LEFT_OPEN_RIGHT_OPEN:
            rangeSet.add(Range.open(left, right));
            break;
        }
      }
      load = true;
      build.build();
    }

    public CompletableFuture<Void> save() {
      Set<Range<Long>> ranges = rangeSet.asRanges();
      ByteBuffer buf = ByteBuffer.allocate((1 + 8 + 8) * ranges.size());
      for (Range<Long> range : ranges) {
        switch (range.lowerBoundType()) {
          case CLOSED:
            switch (range.upperBoundType()) {
              case OPEN:
                buf.put(LEFT_CLOSE_RIGHT_OPEN);
                break;
              case CLOSED:
                buf.put(LEFT_CLOSE_RIGHT_CLOSE);
                break;
            }
            break;
          case OPEN:
            switch (range.upperBoundType()) {
              case OPEN:
                buf.put(LEFT_OPEN_RIGHT_OPEN);
                break;
              case CLOSED:
                buf.put(LEFT_OPEN_RIGHT_CLOSE);
                break;
            }
            break;
        }
        buf.putLong(range.lowerEndpoint());
        buf.putLong(range.upperEndpoint());
      }
      return partition.setMeta(
          ByteBuffer.allocate(8).put(metaKeyPrefix).putInt(index).array(), buf.array());
    }
  }

  enum Status {
    INIT,
    RECOVER,
    START
  }
}
