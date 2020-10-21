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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.superhx.linky.broker.Configuration;
import org.superhx.linky.service.proto.BatchRecord;
import org.superhx.linky.service.proto.Record;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 5)
@State(value = Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
public class JournalTest {
  private static final String path = System.getProperty("user.home") + "/linkytest/mappedfilestest";
  private Journal wal;
  private BatchRecordJournalData batchRecord;

  @Param(value = {"128"})
  private int blockSize = 1024;

  @Setup(value = Level.Iteration)
  public void setUp() {
    wal = new JournalPerf(path, new Configuration());
    wal.init();
    wal.start();
    byte[] data = new byte[blockSize];
    batchRecord =
        new BatchRecordJournalData(
            BatchRecord.newBuilder()
                .addRecords(Record.newBuilder().setValue(ByteString.copyFrom(data)).build())
                .build());
  }

  @TearDown(value = Level.Iteration)
  public void tearDown() {
    wal.shutdown();
    wal.delete();
  }

  @Benchmark
  public void perf() {
    wal.append(batchRecord);
  }

  static class JournalPerf extends AbstractJournal<BatchRecordJournalData> {
    public JournalPerf(String storePath, Configuration configuration) {
      super(storePath, configuration);
    }

    @Override
    public CompletableFuture<AppendResult> append(RecordData record) {
      return super.append(record);
    }

    @Override
    protected Record<BatchRecordJournalData> parse(long offset, ByteBuffer byteBuffer) {
      ByteBuffer data = byteBuffer.slice();
      int size = data.getInt();
      int magicCode = data.getInt();
      if (magicCode == BLANK_MAGIC_CODE) {
        return Record.<BatchRecordJournalData>newBuilder()
            .setBlank(true)
            .setSize(size)
            .setOffset(offset)
            .build();
      }
      try {
        BatchRecord batchRecord = BatchRecord.parseFrom(data);
        return Record.<BatchRecordJournalData>newBuilder()
            .setSize(size)
            .setOffset(offset)
            .setData(new BatchRecordJournalData(batchRecord))
            .build();
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  public static void main(String... args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(JournalTest.class.getSimpleName())
            .result("result.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opt).run();
  }
}
