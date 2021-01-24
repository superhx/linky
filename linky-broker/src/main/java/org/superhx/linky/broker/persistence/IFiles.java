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
import org.superhx.linky.broker.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

public interface IFiles extends Lifecycle {
  Logger log = LoggerFactory.getLogger(IFiles.class);

  AppendResult append(ByteBuffer byteBuffer);

  CompletableFuture<ByteBuffer> read(long position, int size);

  long force();

  long startOffset();

  long writeOffset();

  long confirmOffset();

  void delete();

  IFile getFile(long position);

  void deleteFile(IFile file);

  class Checkpoint {
    private String path;
    private long slo;
    private boolean changed;

    public Checkpoint() {}

    public static Checkpoint load(String path) {
      Checkpoint checkpoint = Utils.file2jsonObj(path, Checkpoint.class);
      if (checkpoint == null) {
        checkpoint = new Checkpoint();
        checkpoint.changed = true;
      }
      checkpoint.path = path;
      return checkpoint;
    }

    public long getSlo() {
      return slo;
    }

    public void setSlo(long slo) {
      this.slo = slo;
      changed = true;
    }

    public void persist() {
      if (changed) {
        Utils.jsonObj2file(this, path);
      }
    }

    public void delete() {
      try {
        Files.delete(Paths.get(path));
      } catch (IOException e) {
      }
    }
  }

  class AppendResult {
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
