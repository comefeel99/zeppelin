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


import java.util.List;
import java.util.Map;

import org.apache.zeppelin.webstore.DocOp;

/**
 * watch need to be invoked in all servers.
 * So related information to invoke watchers need to be broadcasted.
 */
public class InvokeWatchersBroadcastMessage {
  private String traceId;
  private String collection;
  private String id;
  private List<DocOp> ops;
  private Map doc;

  // simple constructor for jackson.
  public InvokeWatchersBroadcastMessage() {
  }

  public InvokeWatchersBroadcastMessage(String traceId, String collection, String id, List<DocOp> ops, Map doc) {
    this.traceId = traceId;
    this.collection = collection;
    this.id = id;
    this.ops = ops;
    this.doc = doc;
  }

  public String getTraceId() {
    return traceId;
  }

  public String getCollection() {
    return collection;
  }

  public String getId() {
    return id;
  }

  public List<DocOp> getOps() {
    return ops;
  }

  public Map getDoc() {
    return doc;
  }
}
