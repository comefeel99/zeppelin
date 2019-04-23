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
package org.apache.zeppelin.webstore;

import org.apache.zeppelin.webrpc.WebRpcServer;
import org.apache.zeppelin.webrpc.annotation.WEBRPC;
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.security.WebRpcAccessRule;
import org.apache.zeppelin.webrpc.security.WebRpcPermissionDeniedException;
import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webstore.pubsub.Pubsub;
import org.apache.zeppelin.webstore.security.WebstoreAcl;
import org.apache.zeppelin.webstore.security.WebstorePermissionDeniedException;
import org.apache.zeppelin.webstore.storage.UpdateConflictException;
import org.apache.zeppelin.webstore.storage.WebstoreStorage;
import org.apache.zeppelin.webstore.storage.WebstoreStorageManager;
import org.apache.zeppelin.webstore.watch.Watch;
import org.apache.zeppelin.webstore.watch.WatchListener;
import org.apache.zeppelin.webstore.watch.WebstoreWatch;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class Webstore {
  private static boolean devMode = false;

  private final WebstoreAcl webstoreAcl;
  private final WebstoreWatch webstoreWatch;
  private final WebstoreStorageManager storageManager;
  private final Pubsub pubsub;
  private final SessionManager sessionManager;
  private final WebRpcServer webrpcServer;

  public Webstore(
          WebRpcServer webrpcServer,
          WebstoreAcl webstoreAcl,
          Pubsub pubsub,
          WebstoreStorageManager storageManager) {
    this.webstoreAcl = webstoreAcl;
    this.webrpcServer = webrpcServer;
    this.storageManager = storageManager;
    this.pubsub = pubsub;
    this.sessionManager = webrpcServer.getSessionManager();
    this.webstoreWatch = new WebstoreWatch(webstoreAcl, pubsub, sessionManager, storageManager);

    registerRpc();
  }

  public WebstoreWatch getWebstoreWatch() {
    return webstoreWatch;
  }

  private void registerRpc() {
    webrpcServer.register(Const.WEBSTORE_RPC_NAME, this);
    webrpcServer.getAcl().allow(new WebRpcAccessRule() {
      @Override
      public boolean check(AclContext aclContext, String rpcName, String methodName, Class[] types, Object[] params, Object rpcObject, boolean permanent) throws WebRpcPermissionDeniedException {
        if (Const.WEBSTORE_RPC_NAME.equals(rpcName)) {
          // allow methods that has WEBRPC annotation.
          for (Method m : Webstore.class.getDeclaredMethods()) {
            if (m.isAnnotationPresent(WEBRPC.class) && methodName.equals(m.getName())) {
              return true;
            }
          }
        }
        return false;
      }
    });
  }

  /**
   * Get document reference.
   * @param ctx
   * @param collection
   * @param id
   * @return
   */
  public DocRef getRef(AclContext ctx, String collection, String id) {
    WebstoreStorage storage = storageManager.get(collection);
    if (storage == null) {
      return null;
    } else {
      return new DocRef(collection, id, webstoreAcl, ctx, webstoreWatch, storage);
    }
  }

  public Collection<DocRef> findRef(AclContext ctx, String collection, String startId, String endId)
          throws WebstorePermissionDeniedException {
    Collection<String> docIds = findDocId(ctx, collection, startId, endId);
    List<DocRef> docRefs = docIds.stream().map(id -> {
      return getRef(ctx, collection, id);
    }).collect(Collectors.toList());
    return docRefs;
  }

  /**
   * Following methods are WEBRPC methods for convenience.
   */
  @WEBRPC
  public Object get(AclContext ctx, String collection, String docId, String jsonPath)
          throws WebstorePermissionDeniedException {
    return getRef(ctx, collection, docId).get(jsonPath, Object.class);
  }

  @WEBRPC
  public Object set(AclContext ctx, String collection, String docId, String jsonPath, Object value)
          throws WebstorePermissionDeniedException, UpdateConflictException {
    return getRef(ctx, collection, docId).set(jsonPath, value);
  }
  @WEBRPC
  public Object delete(AclContext ctx, String collection, String docId, String jsonPath)
          throws WebstorePermissionDeniedException, UpdateConflictException {
    return getRef(ctx, collection, docId).delete(jsonPath);
  }

  public Object delete(AclContext ctx, String collection, String docId) throws WebstorePermissionDeniedException, UpdateConflictException {
    return delete(ctx, collection, docId, Const.ROOT_PATH);
  }

  @WEBRPC
  public Object apply(AclContext ctx, String collection, String docId, List<DocOp> ops, String actionName)
          throws WebstorePermissionDeniedException, UpdateConflictException {
    return getRef(ctx, collection, docId).apply(ops, actionName);
  }

  @WEBRPC
  public void watch(AclContext ctx, Session session, String watchId,
                    String collection, String docId, String jsonPath)
          throws WebstorePermissionDeniedException {
    webstoreWatch.addWatch(
            ctx,
            session.getId(),
            watchId,
            new Watch(collection, docId, jsonPath, new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
                session.rpcInvoke(
                        Const.WEBSTORE_RPC_NAME,
                        "onChange",
                        new Object[]{watchId, collection, docId, path, jsonObject});
              }
            })
    );

    // call watch callback after registration.
    DocRef docRef = getRef(ctx, collection, docId);
    Object doc = docRef.get(jsonPath, Object.class);
    session.rpcInvoke(
            Const.WEBSTORE_RPC_NAME,
            "onChange",
            new Object[]{watchId, collection, docId, jsonPath, doc});
  }

  @WEBRPC
  public void removeWatch(Session session, String watchId) {
    webstoreWatch.removeWatch(session.getId(), watchId);
  }

  @WEBRPC
  public Collection<String> findDocId(AclContext ctx, String collection, String startId, String endId)
          throws WebstorePermissionDeniedException {
    webstoreAcl.checkFind(ctx, collection, startId, endId);
    WebstoreStorage storage = storageManager.get(collection);
    if (storage == null) {
      return null;
    } else {
      return storage.find(collection, startId, endId);
    }
  }

  public static boolean isDevMode() {
    return devMode;
  }

  public static void setDevMode(boolean tf) {
    Webstore.devMode = tf;
  }
}
