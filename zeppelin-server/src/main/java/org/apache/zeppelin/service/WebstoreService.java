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
package org.apache.zeppelin.service;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.zeppelin.notebook.socket.Message;
import org.apache.zeppelin.socket.ConnectionManager;
import org.apache.zeppelin.socket.NotebookServer;
import org.apache.zeppelin.socket.NotebookSocket;
import org.apache.zeppelin.socket.NotebookSocketListener;
import org.apache.zeppelin.webrpc.WebRpcServer;
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.security.WebRpcAcl;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionListener;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webrpc.transport.SessionManagerListener;
import org.apache.zeppelin.webrpc.transport.ws.WebsocketSession;
import org.apache.zeppelin.webstore.Webstore;
import org.apache.zeppelin.webstore.pubsub.Topic;
import org.apache.zeppelin.webstore.pubsub.memory.MemoryPubsub;
import org.apache.zeppelin.webstore.security.WebstoreAcl;
import org.apache.zeppelin.webstore.storage.WebstoreStorageManager;
import org.apache.zeppelin.webstore.storage.memory.MemoryStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class WebstoreService implements SessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebstoreService.class);
  private static Gson gson = new Gson();
  private final NotebookServer notebookServer;
  private final WebRpcServer webrpc;
  private final Webstore webstore;
  private final MemoryPubsub pubsub;
  private final MemoryStorage memoryStorage;
  List<SessionManagerListener> sessionManagerListeners = Collections.synchronizedList(new LinkedList());
  WebRpcAcl webRpcAcl = new WebRpcAcl();
  WebstoreAcl webstoreAcl = new WebstoreAcl();
  WebstoreStorageManager storageManager;

  @Inject
  public WebstoreService(NotebookServer notebookServer) {
    this.notebookServer = notebookServer;

    pubsub = new MemoryPubsub();
    storageManager = new WebstoreStorageManager();
    memoryStorage = new MemoryStorage();
    storageManager.add(".*", memoryStorage);

    webrpc = new WebRpcServer(this, webRpcAcl);
    webstore = new Webstore(webrpc, webstoreAcl, pubsub, storageManager);
  }

  @Override
  public Session getSession(String sessionId) {
    ConnectionManager connectionManager = notebookServer.getConnectionManager();
    NotebookSocket conn = connectionManager.getConnection(sessionId);
    if (conn == null) {
      return null;
    } else {
      return new ZeppelinWebrpcSession(conn);
    }
  }

  @Override
  public AclContext createAclContext(String sessionId) {
    ConnectionManager connectionManager = notebookServer.getConnectionManager();
    NotebookSocket conn = connectionManager.getConnection(sessionId);

    AclContext aclContext = new AclContext();
    aclContext.put("user", conn.getUser());
    aclContext.put("conn", conn);
    return aclContext;
  }

  @Override
  public void addListener(SessionManagerListener listener) {
    sessionManagerListeners.add(listener);
  }

  class ZeppelinWebrpcSession implements Session, NotebookSocketListener {
    private final NotebookSocket notebookSocket;
    private final NotebookSocketListener notebookServerListener;
    private SessionListener listener;

    ZeppelinWebrpcSession(NotebookSocket notebookSocket) {
      this.notebookSocket = notebookSocket;

      // replace listener
      notebookServerListener = notebookSocket.getListener();
      notebookSocket.setListener(this);

      sessionManagerListeners.stream().forEach(l -> l.onSessionConnect(this));
    }

    @Override
    public String getId() {
      return notebookSocket.toString();
    }

    @Override
    public void rpcInvoke(String rpcName, String methodName, Object[] params) {
      WebsocketSession.ServerToClientInvokeMessage m =
              new WebsocketSession.ServerToClientInvokeMessage(rpcName, methodName, params);
      try {
        notebookSocket.send(gson.toJson(m));
      } catch (IOException e) {
        LOGGER.error("Failed to send rpc invoke", e);
      }
    }

    @Override
    public void sendRpcReturn(String invokeId, Object value) {
      WebsocketSession.ServerToClientReturnMessage m =
              new WebsocketSession.ServerToClientReturnMessage(invokeId, value);
      try {
        notebookSocket.send(gson.toJson(m));
      } catch (IOException e) {
        LOGGER.error("Failed to send rpc return", e);
      }
    }

    @Override
    public void sendRpcException(String invokeId, String errorMessage, Exception e) {
      WebsocketSession.ServerToClientExceptionMessage m =
              new WebsocketSession.ServerToClientExceptionMessage(invokeId, errorMessage, e);
      try {
        notebookSocket.send(gson.toJson(m));
      } catch (IOException er) {
        LOGGER.error("Failed to send rpc return", er);
      }
    }

    @Override
    public void setListener(SessionListener listener) {
      this.listener = listener;
    }

    @Override
    public void onClose(NotebookSocket socket, int code, String message) {
      notebookServerListener.onClose(socket, code, message);
      sessionManagerListeners.stream().forEach(l -> l.onSessionDisconnect(this));
    }

    @Override
    public void onOpen(NotebookSocket socket) {
      notebookServerListener.onOpen(socket);
    }

    @Override
    public boolean onMessage(NotebookSocket socket, String message) {
      if (!notebookServerListener.onMessage(socket, message)) {
        Message m = gson.fromJson(message, Message.class);

        if (m.op == Message.OP.RPC_INVOKE) {
          WebsocketSession.ClientToServerInvokeMessage invoke
                  = gson.fromJson(gson.toJson(m.data), WebsocketSession.ClientToServerInvokeMessage.class);

          listener.onRpcInvoke(
                  this,
                  invoke.getInvokeId(),
                  invoke.getRpcName(),
                  invoke.getMethodName(),
                  invoke.getParams(),
                  invoke.getTypes());
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }
  }
}
