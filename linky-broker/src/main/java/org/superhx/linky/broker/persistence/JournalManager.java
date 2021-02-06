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

import org.superhx.linky.broker.Configuration;
import org.superhx.linky.broker.Lifecycle;
import org.superhx.linky.broker.LinkyIOException;
import org.superhx.linky.broker.Utils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JournalManager implements Lifecycle {
  private Map<String, Journal> journals = new HashMap<>();

  public JournalManager(String path) {
    String dataDirPath = path + "/data";
    Utils.ensureDirOK(dataDirPath + "/linky/logs");
    File dataDir = new File(dataDirPath);
    File[] linkys = dataDir.listFiles();
    if (linkys == null) {
      throw new LinkyIOException(String.format("%s is not directory", dataDirPath));
    }
    for (File linky : linkys) {
      if (linky.isFile()) {
        continue;
      }
      if (linky.getName().startsWith("linky")) {
        String journalPath = linky.getPath() + "/logs";
        Journal journal = new JournalImpl(journalPath, new Configuration());
        journals.put(linky.getPath(), journal);
      }
    }
  }

  @Override
  public void init() {
    journals.values().forEach(j -> j.init());
  }

  @Override
  public void start() {
    journals.values().forEach(j -> j.start());
  }

  @Override
  public void shutdown() {
    journals.values().forEach(j -> j.shutdown());
  }

  public Map<String, Journal> journals() {
    return Collections.unmodifiableMap(journals);
  }

  public Journal journal(String path) {
    return journals.get(path);
  }
}
