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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.superhx.linky.broker.Configuration;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 5)
@State(value = Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
public class JournalPerfTest {
  private static final String path = System.getProperty("user.home") + "/linkytest/mappedfilestest";
  private Journal wal;
  private Journal.BytesData bytesData;

  @Param(value = {"128"})
  private int blockSize = 1024;

  @Setup(value = Level.Iteration)
  public void setUp() {
    wal = new JournalImpl(path, new Configuration());
    wal.init();
    wal.start();
    byte[] data = new byte[blockSize];
    bytesData = new Journal.BytesData(data);
  }

  @TearDown(value = Level.Iteration)
  public void tearDown() {
    wal.shutdown();
    wal.delete();
  }

  @Benchmark
  public void perf() {
    wal.append(bytesData, (r) -> {});
  }

  public static void main(String... args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(JournalPerfTest.class.getSimpleName())
            .result("result.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opt).run();
  }
}
