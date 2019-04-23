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
package org.apache.zeppelin.webrpc;

import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.security.WebRpcAcl;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionListener;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webrpc.transport.SessionManagerListener;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WebRpcServer implements SessionListener, SessionManagerListener {
  private static final String GLOBAL = "__GLOBAL__";
  private final SessionManager sessionManager;
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Object>> rpcMap = new ConcurrentHashMap<>();
  private final WebRpcAcl webRpcAcl;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  public WebRpcServer(SessionManager sessionManager,
                      WebRpcAcl webRpcAcl) {
    this.sessionManager = sessionManager;
    this.webRpcAcl = webRpcAcl;

    sessionManager.addListener(this);
  }

  RpcRef getRpcRef(AclContext aclContext, Session session, String rpcName) {
    return new RpcRef(this, webRpcAcl, aclContext, session, rpcName);
  }

  /**
   * Register a global rpc object that is available for all sessions.
   * @param rpcName
   * @param rpcObject
   */
  public void register(String rpcName, Object rpcObject) {
    register(GLOBAL, rpcName, rpcObject);
  }

  /**
   * Unregister a global rpc object that is available for all sessions.
   * This method does not removes any rpc object registered to a specific session, even if rpcName is the same.
   * @param rpcName
   */
  public void unregister(String rpcName) {
    unregister(GLOBAL, rpcName);
  }

  /**
   * Register a rpc object that attached to a specific session.
   * The registered rpcName is not be available in other sessions.
   * On session disconnect, the rpcName will be automatically unregistered.
   * This overrides the rpcName already registered for all sessions when rpcName is the same.
   * @param sessionId
   * @param rpcName
   * @param rpcObject
   */
  public void register(String sessionId, String rpcName, Object rpcObject) {
    rpcMap.compute(sessionId, (sId, value) -> {
      ConcurrentHashMap<String, Object> perSessionMap;
      if (value == null) {
        perSessionMap = new ConcurrentHashMap<>();
      } else {
        perSessionMap = value;
      }

      perSessionMap.put(rpcName, rpcObject);
      return perSessionMap;
    });
  }

  /**
   * Unregister rpcName registered to a specific session.
   * All rpcName registered to a specific session will be unregistered automatically on session disconnect.
   * @param sessionId
   * @param rpcName
   */
  public void unregister(String sessionId, String rpcName) {
    rpcMap.computeIfPresent(sessionId, (sId, perSessionMap) -> {
      perSessionMap.remove(rpcName);
      if (perSessionMap.isEmpty()) {
        return null;
      } else {
        return perSessionMap;
      }
    });
  }

  Object getRpcObject(String rpcName) {
    ConcurrentHashMap<String, Object> permanentMap = rpcMap.getOrDefault(GLOBAL, null);
    if (permanentMap != null) {
      return permanentMap.getOrDefault(rpcName, null);
    } else {
      return null;
    }
  }

  Object getRpcObject(String sessionId, String rpcName) {
    ConcurrentHashMap<String, Object> perSessionMap = rpcMap.getOrDefault(sessionId, null);
    if (perSessionMap != null) {
      return perSessionMap.getOrDefault(rpcName, null);
    } else {
      return null;
    }
  }

  @Override
  public void onSessionConnect(Session session) {
    session.setListener(this);
  }

  @Override
  public void onSessionDisconnect(Session session) {
    // unregister all
    rpcMap.remove(session.getId());
  }

  @Override
  public void onRpcInvoke(Session session, String invokeId, String rpcName, String methodName, Object[] params, String[] types) {
    AclContext aclContext = sessionManager.createAclContext(session.getId());
    final RpcRef ref = getRpcRef(aclContext, session, rpcName);
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Object ret = ref.invoke(methodName, types, params);
          session.sendRpcReturn(invokeId, ret);
        } catch (Exception e) {
          session.sendRpcException(
                  invokeId,
                  String.format("Invocation (%s) %s.%s failed", invokeId, rpcName, methodName),
                  e);
        }
      }
    });
  }

  public WebRpcAcl getAcl() {
    return webRpcAcl;
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }
}
