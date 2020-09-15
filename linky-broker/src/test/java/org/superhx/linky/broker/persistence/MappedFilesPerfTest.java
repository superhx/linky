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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 60)
@State(value = Scope.Benchmark)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
public class MappedFilesPerfTest {
  private static final String path = System.getProperty("user.home") + "/linkytest/mappedfilestest";
  private MappedFiles mappedFiles;

  @Param(value = {"128"})
  private int blockSize = 1024;

  private byte[] data;

  @Setup(value = Level.Iteration)
  public void setUp() {
    mappedFiles = new MappedFiles(path, 1024 * 1024 * 1024, (mappedFile, lso) -> lso, null);
    mappedFiles.init();
    mappedFiles.start();

    data = new byte[blockSize];
    new Random().nextBytes(data);
  }

  @TearDown(value = Level.Iteration)
  public void tearDown() {
    mappedFiles.delete();
    mappedFiles = null;
  }

  @Benchmark
  public void perf() {
    mappedFiles.append(ByteBuffer.wrap(data));
  }

  public static void main(String... args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(MappedFilesPerfTest.class.getSimpleName())
            .result("result.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opt).run();
  }
}
