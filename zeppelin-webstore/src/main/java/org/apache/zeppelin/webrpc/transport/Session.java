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

public interface Session {
  /**
   * Return unique sessionId.
   * @return
   */
  String getId();

  /**
   * Invoke client method. Does not support getting return value for now.
   * @param rpcName
   * @param methodName
   * @param params
   */
  void rpcInvoke(String rpcName, String methodName, Object [] params);

  /**
   * Send return value of invocation.
   * @param invokeId
   * @param value
   */
  void sendRpcReturn(String invokeId, Object value);

  /**
   * Send exception of invocation.
   * @param invokeId
   * @param errorMessage
   * @param e
   */
  void sendRpcException(String invokeId, String errorMessage, Exception e);


  /**
   * Set session listener.
   * This class implementation should invoke listener.onRpcInvoke() on invoke message received from client.
   * @param listener
   */
  void setListener(SessionListener listener);
}
