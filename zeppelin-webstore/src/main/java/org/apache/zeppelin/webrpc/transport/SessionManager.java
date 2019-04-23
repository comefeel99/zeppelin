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
package org.apache.zeppelin.webrpc.transport;

import org.apache.zeppelin.webrpc.security.AclContext;

public interface SessionManager {
  /**
   * Get session by sessionId.
   * @param sessionId
   * @return null if session does not exists
   */
  Session getSession(String sessionId);

  /**
   * Create new AclContext for the session.
   * @param sessionId
   * @return
   */
  AclContext createAclContext(String sessionId);

  /**
   * SessionManager implementation should invoke listener.onSessionConnect(), listener.onSessionDisconnect()
   * on new session connection or new session disconnection, for all listeners.
   * @param listener
   */
  void addListener(SessionManagerListener listener);
}
