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
package org.apache.zeppelin.webstore.watch;

/**
 * Watch information.
 */
public class Watch {
  private final String collection;
  private final String id;
  private final String path;
  private final WatchListener listener;

  public Watch(String collection, String id, String path, WatchListener listener) {
    this.collection = collection;
    this.id = id;
    this.path = path;
    this.listener = listener;
  }

  public String getCollection() {
    return collection;
  }

  public String getId() {
    return id;
  }

  public String getPath() {
    return path;
  }

  public WatchListener getListener() {
    return listener;
  }
}
