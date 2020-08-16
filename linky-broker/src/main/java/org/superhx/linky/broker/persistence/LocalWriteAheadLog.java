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
import org.superhx.linky.service.proto.BatchRecord;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class LocalWriteAheadLog implements WriteAheadLog {
  public static final int RECORD_MAGIC_CODE = -19951994;
  public static final int BLANK_MAGIC_CODE = -2333;
  private static final Logger log = LoggerFactory.getLogger(LocalSegmentManager.class);
  private static final int fileSize = 1024 * 1024 * 1024;
  private MappedFiles mappedFiles;

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
            });
  }

  @Override
  public CompletableFuture<AppendResult> append(BatchRecord batchRecord) {
    byte[] data = batchRecord.toByteArray();
    int size = 4 + 4 + data.length;
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    byteBuffer.putInt(size);
    byteBuffer.putInt(RECORD_MAGIC_CODE);
    byteBuffer.put(data);
    byteBuffer.flip();
    return mappedFiles.write(byteBuffer).thenApply(o -> new AppendResult(o, size));
  }

  @Override
  public CompletableFuture<BatchRecord> get(long offset, int size) {
    return mappedFiles
        .read(offset, size)
        .thenApply(
            b -> {
              ByteBuffer data = b.slice();
              data.getInt();
              data.getInt();
              try {
                return BatchRecord.parseFrom(data);
              } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
              }
              return null;
            });
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
