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
package org.superhx.linky;

import org.rocksdb.RocksDBException;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RocksDBTTest {
  public static void main(String... args) throws RocksDBException {
    if (true) {
      System.out.println(new Date(1613757692000L));
      return;
    }
    //    System.out.println(new Date(Long.MAX_VALUE));
    //    System.out.println(new Date(System.currentTimeMillis()));
    //
    //    RocksDB.loadLibrary();
    //    DBOptions options =
    //        new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    //    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
    //    final List<ColumnFamilyDescriptor> cfDescriptors =
    //        Arrays.asList(
    //            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
    //            new ColumnFamilyDescriptor("INDEX".getBytes(), cfOptions));
    //    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    //    RocksDB db =
    //        RocksDB.open(options, "/Users/wumu.hx/linky", cfDescriptors, columnFamilyHandleList);
    //    //    WriteBatch writeBatch = new WriteBatch();
    //    //    writeBatch.put(
    //    //        "hello11".getBytes(Utils.DEFAULT_CHARSET),
    // "world11".getBytes(Utils.DEFAULT_CHARSET));
    //    //    writeBatch.put(
    //    //            "hello21".getBytes(Utils.DEFAULT_CHARSET),
    //    // "world21".getBytes(Utils.DEFAULT_CHARSET));
    //    //    writeBatch.put(
    //    //            "hello12".getBytes(Utils.DEFAULT_CHARSET),
    //    // "world12".getBytes(Utils.DEFAULT_CHARSET));
    //    //    writeBatch.put(
    //    //            "hello18".getBytes(Utils.DEFAULT_CHARSET),
    //    // "world18".getBytes(Utils.DEFAULT_CHARSET));
    //    //    writeBatch.put(
    //    //            "hello19".getBytes(Utils.DEFAULT_CHARSET),
    //    // "world19".getBytes(Utils.DEFAULT_CHARSET));
    //    //    db.write(new WriteOptions(), writeBatch);
    //    //    db.put("hello".getBytes(Utils.DEFAULT_CHARSET),
    // "world1".getBytes(Utils.DEFAULT_CHARSET));
    //    db.put(
    //        columnFamilyHandleList.get(1),
    //        "hello".getBytes(Utils.DEFAULT_CHARSET),
    //        "world2".getBytes(Utils.DEFAULT_CHARSET));
    //    //    System.out.println(new String(db.get("hello".getBytes(Utils.DEFAULT_CHARSET))));
    //    //    System.out.println(
    //    //        new String(db.get(columnFamilyHandleList.get(1),
    //    // "hello".getBytes(Utils.DEFAULT_CHARSET))));
    //
    //    RocksIterator it = db.newIterator(columnFamilyHandleList.get(1));
    //    for (; it.isValid(); it.next()) {
    //      System.out.println(new String(it.key()));
    //    }
    //    it.close();
    //    //    it.seekForPrev("hello19".getBytes(Utils.DEFAULT_CHARSET));
    //    //    System.out.println(new String( it.value()));
    //  }

    CompletableFuture<Void> f1 = new CompletableFuture<>();
    new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  Thread.sleep(10000);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                System.out.println("f1" + new Date());
                f1.complete(null);
              }
            })
        .start();

    CompletableFuture<Void> f2 = new CompletableFuture<>();
    f2.completeExceptionally(new RuntimeException());

    try {
      CompletableFuture.allOf(f1, f2).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      System.out.println(new Date());
      e.printStackTrace();
    }
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
