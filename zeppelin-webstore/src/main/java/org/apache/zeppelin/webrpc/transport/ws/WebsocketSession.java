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

import com.google.gson.Gson;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionListener;

import javax.websocket.MessageHandler;

public class WebsocketSession implements Session, MessageHandler.Whole<String> {
  private final javax.websocket.Session ws;
  private SessionListener listener;
  private final Gson gson = new Gson();

  public static class ServerToClientInvokeMessage {
    private final String type = "RPC_INVOKE";
    private final String rpcName;
    private final String methodName;
    private final Object[] params;

    public ServerToClientInvokeMessage(String rpcName, String methodName, Object[] params) {
      this.rpcName = rpcName;
      this.methodName = methodName;
      this.params = params;
    }
  }

  public static class ServerToClientReturnMessage {
    private final String type = "RPC_RETURN";
    private final String invokeId;
    private final Object value;

    public ServerToClientReturnMessage(String invokeId, Object value) {
      this.invokeId = invokeId;
      this.value = value;
    }
  }

  public static class ServerToClientExceptionMessage {
    private final String type = "RPC_EXCEPTION";
    private final String invokeId;
    private final String errorMessage;
    private final String exception;

    public ServerToClientExceptionMessage(String invokeId, String errorMessage, Exception e) {
      this.invokeId = invokeId;
      this.errorMessage = errorMessage;
      this.exception = (e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
    }
  }

  public static class ClientToServerInvokeMessage {
    private final String invokeId = null;
    private final String rpcName = null;
    private final String methodName = null;
    private final Object [] params = null;
    private final String [] types = null;

    public String getInvokeId() {
      return invokeId;
    }

    public String getRpcName() {
      return rpcName;
    }

    public String getMethodName() {
      return methodName;
    }

    public Object[] getParams() {
      return params;
    }

    public String[] getTypes() {
      return types;
    }
  }

  public WebsocketSession(javax.websocket.Session session) {
    this.ws = session;
    ws.addMessageHandler(this);
  }

  @Override
  public String getId() {
    return ws.getId();
  }

  @Override
  public void rpcInvoke(String rpcName, String methodName, Object[] params) {
    ServerToClientInvokeMessage m = new ServerToClientInvokeMessage(rpcName, methodName, params);
    ws.getAsyncRemote().sendText(gson.toJson(m));
  }

  @Override
  public void sendRpcReturn(String invokeId, Object value) {
    ServerToClientReturnMessage m = new ServerToClientReturnMessage(invokeId, value);
    ws.getAsyncRemote().sendText(gson.toJson(m));
  }

  @Override
  public void sendRpcException(String invokeId, String errorMessage, Exception e) {
    ServerToClientExceptionMessage m = new ServerToClientExceptionMessage(invokeId, errorMessage, e);
    ws.getAsyncRemote().sendText(gson.toJson(m));
  }

  @Override
  public void setListener(SessionListener listener) {
    this.listener = listener;
  }

  @Override
  public void onMessage(String message) {
    ClientToServerInvokeMessage invoke = gson.fromJson(message, ClientToServerInvokeMessage.class);
    listener.onRpcInvoke(
            this,
            invoke.getInvokeId(),
            invoke.getRpcName(),
            invoke.getMethodName(),
            invoke.getParams(),
            invoke.getTypes());
  }

  public javax.websocket.Session getRawSession() {
    return ws;
  }
}
