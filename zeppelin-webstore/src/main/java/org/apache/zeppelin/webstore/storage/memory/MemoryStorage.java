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

package org.apache.zeppelin.webstore.storage.memory;

import com.google.gson.Gson;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.zeppelin.webstore.storage.ChangeFeedListener;
import org.apache.zeppelin.webstore.storage.UpdateConflictException;
import org.apache.zeppelin.webstore.storage.WebstoreNotFoundException;
import org.apache.zeppelin.webstore.storage.WebstoreStorage;

public class MemoryStorage implements WebstoreStorage {
  private final Gson gson = new Gson();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Object>> storage = new ConcurrentHashMap<>();

  @Override
  public void update(String collection, String docId, Object doc) throws UpdateConflictException {
    storage.compute(collection, (k, v) -> {
      if (v == null) {
        v = new ConcurrentHashMap<>();
      }

      v.put(docId, doc);
      return v;
    });
  }

  @Override
  public Map get(String collection, String docId) throws WebstoreNotFoundException {
    ConcurrentHashMap<String, Object> c = storage.get(collection);
    if (c == null) {
      throw new WebstoreNotFoundException(String.format("%s not exists", docId));
    }

    Object doc = c.get(docId);
    if (doc == null) {
      throw new WebstoreNotFoundException(String.format("%s not exists", docId));
    }

    return (Map) doc;
  }

  @Override
  public void delete(String collection, String docId) {
    storage.computeIfPresent(collection, (k, v) -> {
      v.remove(docId);
      return v;
    });
  }

  @Override
  public boolean exists(String collection, String docId) {
    ConcurrentHashMap<String, Object> c = storage.get(collection);
    return c != null && c.containsKey(docId);
  }

  @Override
  public Collection<String> find(String collection, String startDocId, String endDocId) {
    ConcurrentHashMap<String, Object> c = storage.get(collection);
    if (c == null) {
      return new LinkedList<>();
    }

    return c.entrySet().stream().filter(entry -> {
      if (entry.getKey().compareTo(startDocId) >= 0 &&
              entry.getKey().compareTo(endDocId) <= 0) {
        return true;
      } else {
        return false;
      }
    }).sorted(new Comparator<Map.Entry<String, Object>>() {
      @Override
      public int compare(Map.Entry<String, Object> o1, Map.Entry<String, Object> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    }).map(e -> e.getKey()).collect(Collectors.toList());
  }

  @Override
  public boolean changeFeedSupported() {
    return false;
  }

  @Override
  public void setChangeFeedListener(ChangeFeedListener listener) {
  }
}
