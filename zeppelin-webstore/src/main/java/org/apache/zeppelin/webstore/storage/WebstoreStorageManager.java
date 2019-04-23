/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.webstore.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebstoreStorageManager {
  LinkedList<Map<Pattern, WebstoreStorage>> storages = new LinkedList<>();

  public WebstoreStorageManager() {
  }

  /**
   * add storage.
   * @param collectionRegex regex pattern to match collection name
   * @param storage storage to use
   */
  public void add(String collectionRegex, WebstoreStorage storage) {
    HashMap<Pattern, WebstoreStorage> entry = new HashMap<>();
    entry.put(Pattern.compile(collectionRegex), storage);
    storages.add(entry);
  }

  /**
   * Get storage for collection.
   * @param collection
   * @return
   */
  public WebstoreStorage get(String collection) {
    for (Map<Pattern, WebstoreStorage> entry : storages) {
      for (Map.Entry<Pattern, WebstoreStorage> e : entry.entrySet()) {
        Matcher m = e.getKey().matcher(collection);
        if (m.matches()) {
          return e.getValue();
        }
      }
    }
    return null;
  }
}
