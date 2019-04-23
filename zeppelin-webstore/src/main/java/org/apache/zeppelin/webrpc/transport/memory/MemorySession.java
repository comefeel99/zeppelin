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
package org.apache.zeppelin.webrpc.transport.memory;

import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MemorySession implements Session {
  private final String loginName;
  private final String id;
  private SessionListener listener;
  private Map<String, Map<String, Object>> ret = new HashMap<>();
  int nextId = 0;
  private boolean shutdown = false;

  public MemorySession(String loginName) {
    this.loginName = loginName;
    this.id = UUID.randomUUID().toString().replaceAll("-", "");
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void sendRpcReturn(String invokeId, Object value) {
    Map<String, Object> result = new HashMap<>();
    result.put("value", value);
    synchronized (ret) {
      ret.put(invokeId, result);
      ret.notify();
    }
  }

  @Override
  public void sendRpcException(String invokeId, String errorMessage, Exception e) {
    Map<String, Object> result = new HashMap<>();
    result.put("errorMessage", errorMessage);
    result.put("exception", e);
    e.printStackTrace();
    synchronized (ret) {
      ret.put(invokeId, result);
      ret.notify();
    }
  }

  @Override
  public void setListener(SessionListener listener) {
    this.listener = listener;
  }

  @Override
  public void rpcInvoke(String rpcName, String methodName, Object[] params) {

  }

  public String loginName() {
    return loginName;
  }

  public Map<String, Object> rpcReturn(String invokeId) {
    synchronized (ret) {
      while (!ret.containsKey(invokeId) && !shutdown) {
        try {
          ret.wait();
        } catch (InterruptedException e) {
        }
      }
      if (shutdown) {
        return null;
      }

      return ret.get(invokeId);
    }
  }

  public String invoke(String rpcName, String methodName, Object [] params, String [] types) {
    String invokeId = Integer.toString(++nextId);
    listener.onRpcInvoke(this, invokeId, rpcName, methodName, params, types);
    return invokeId;
  }

  public void close() {
    synchronized (ret) {
      shutdown = true;
      ret.notify();
    }
  }
}
