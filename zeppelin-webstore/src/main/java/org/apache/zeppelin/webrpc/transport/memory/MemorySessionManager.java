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

import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webrpc.transport.SessionManagerListener;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class MemorySessionManager implements SessionManager {
  ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
  LinkedList<SessionManagerListener> listeners = new LinkedList();

  /**
   * Create new session
   * @param loginName
   * @return
   */
  public Session open(String loginName) {
    MemorySession sess = new MemorySession(loginName);
    sessions.put(sess.getId(), sess);
    listeners.stream().forEach(l -> {
      l.onSessionConnect(sess);
    });
    return sess;
  }

  /**
   * Destroy session
   * @param sessionId
   */
  public void close(String sessionId) {
    Session sess = sessions.remove(sessionId);
    listeners.stream().forEach(l -> {
      l.onSessionDisconnect(sess);
    });
  }

  @Override
  public Session getSession(String sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  public AclContext createAclContext(String sessionId) {
    AclContext aclContext = new AclContext();
    MemorySession sess = (MemorySession) getSession(sessionId);
    aclContext.put("loginName", sess.loginName());
    return aclContext;
  }

  @Override
  public void addListener(SessionManagerListener listener) {
    listeners.add(listener);
  }
}
