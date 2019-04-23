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
package org.apache.zeppelin.webrpc.transport.ws;
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webrpc.transport.SessionManagerListener;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.websocket.CloseReason;
import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;

public class WebsocketSessionManager extends Endpoint implements SessionManager {
  List<SessionManagerListener> listeners = Collections.synchronizedList(new LinkedList());
  Map<String, WebsocketSession> sessions = new ConcurrentHashMap<>();

  public WebsocketSessionManager() {

  }

  @Override
  public Session getSession(String sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Developers may override this method to add more informations to AclContext, such as login, role, etc.
   * The information can be used
   * @param sessionId
   * @return
   */
  @Override
  public AclContext createAclContext(String sessionId) {
    AclContext aclContext = new AclContext();
    aclContext.put("sessionId", sessionId);

    WebsocketSession s = (WebsocketSession) getSession(sessionId);
    aclContext.put("ws", s.getRawSession());
    return aclContext;
  }

  @Override
  public void addListener(SessionManagerListener listener) {
    listeners.add(listener);
  }

  @Override
  public void onOpen(javax.websocket.Session session, EndpointConfig config) {
    WebsocketSession s = new WebsocketSession(session);
    sessions.put(s.getId(), s);
    listeners.stream().forEach(l -> l.onSessionConnect(s));
  }

  @Override
  public void onClose(javax.websocket.Session ws, CloseReason close) {
    super.onClose(ws, close);
    WebsocketSession s = sessions.remove(ws.getId());
    listeners.stream().forEach(l -> l.onSessionDisconnect(s));
  }

  @Override
  public void onError(javax.websocket.Session ws, Throwable thr) {
    super.onError(ws, thr);
    WebsocketSession s = sessions.remove(ws.getId());
    listeners.stream().forEach(l -> l.onSessionDisconnect(s));
  }
}
