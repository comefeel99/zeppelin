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


import org.apache.zeppelin.webrpc.transport.Session;
import org.apache.zeppelin.webrpc.transport.SessionManager;
import org.apache.zeppelin.webrpc.transport.SessionManagerListener;
import org.apache.zeppelin.webstore.Webstore;
import org.apache.zeppelin.webstore.pubsub.Pubsub;
import org.apache.zeppelin.webstore.pubsub.Subscriber;
import org.apache.zeppelin.webstore.pubsub.Topic;
import org.apache.zeppelin.webstore.storage.ChangeFeedListener;
import org.apache.zeppelin.webstore.storage.WebstoreStorage;
import org.apache.zeppelin.webstore.storage.WebstoreStorageManager;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webstore.Const;
import org.apache.zeppelin.webstore.DocOp;
import org.apache.zeppelin.webstore.security.WebstoreAcl;
import org.apache.zeppelin.webstore.security.WebstorePermissionDeniedException;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage watches.
 */
public class WebstoreWatch implements Subscriber<InvokeWatchersBroadcastMessage>, SessionManagerListener, ChangeFeedListener {
  private static final Logger logger = LoggerFactory.getLogger(WebstoreWatch.class);

  /**
   * <SessionId, <WatchId, Watch> >.
   */
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Watch>> watches
          = new ConcurrentHashMap<>();
  private final WebstoreAcl webstoreACL;
  private final Pubsub pubsub;
  private final SessionManager sessionManager;
  private final ScheduledThreadPoolExecutor threadPoolExecutor;
  private final WebstoreStorageManager storageManager;
  private Topic topic;
  private Gson gson = new Gson();
  final ConcurrentHashMap<String, ScheduledFuture> broadcastVerifier = new ConcurrentHashMap<>();
  private int pubsubRestartTimeoutSec = Const.WEBSTORE_WATCH_PUBSUB_TIMEOUT_SEC;

  public WebstoreWatch(WebstoreAcl webstoreACL, Pubsub pubsub,
                       SessionManager sessionManager,
                       WebstoreStorageManager storageManager) {
    this.webstoreACL = webstoreACL;
    this.pubsub = pubsub;
    this.sessionManager = sessionManager;
    this.storageManager = storageManager;
    this.sessionManager.addListener(this);
    threadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    threadPoolExecutor.setRemoveOnCancelPolicy(true);
    listenPubsub();
  }


  /**
   * Add watch associated to the session.
   * A single client watches multiple documents. And it's very common that
   * the client lost connection (e.g. browser close). So often we need to clean up
   * all the watches of specific client. That's why this method receive sessionId as argument.
   *
   * SET and DELETE operation triggers watch but not GET.
   * To watch document, you need GET permission on document and the path.
   *
   */
  public void addWatch(
          AclContext aclCtx, String sessionId, String watchId, Watch watch)
          throws WebstorePermissionDeniedException {
    // check rule. to watch object, GET permission is required.
    webstoreACL.check(
            aclCtx,
            watch.getCollection(),
            watch.getId(),
            DocOp.get(watch.getPath()),
            null, null);

    watches.compute(sessionId, (k, v) -> {
      ConcurrentHashMap watchersIntheSession;
      if (v == null) {
        watchersIntheSession = new ConcurrentHashMap<>();
      } else {
        watchersIntheSession = v;
      }

      watchersIntheSession.put(watchId, watch);
      logger.info("WatchId {} added in session {}", watchId, sessionId);
      return watchersIntheSession;
    });
  }

  /**
   * Remove all watches associated to the session.
   * @param sessionId
   */
  public void removeWatch(String sessionId) {
    watches.remove(sessionId);
  }

  /**
   * Remove a watch associated to the session.
   * @param sessionId
   * @param watchId
   */
  public void removeWatch(String sessionId, String watchId) {
    watches.computeIfPresent(sessionId, (k, v) -> {
      v.remove(watchId);
      return v;
    });
  }

  /**
   * WebRpcInvokeMessage watchers.
   * @param collection
   * @param id
   * @param ops
   * @param doc
   */
  private void invokeWatchers(String collection, String id, List<DocOp> ops, Map doc) {
    // store matched watcher in this map to prevent multiple call.
    ConcurrentHashMap<String, Watch> watchersToBeCalled = new ConcurrentHashMap<>();

    ops.forEach((op) -> {
      watches.forEachEntry(5, (sessionEntry) -> {
        String sessionId = sessionEntry.getKey();
        ConcurrentHashMap<String, Watch> watches = sessionEntry.getValue();

        watches.forEachEntry(5, (watchEntry) -> {
          String watchId = watchEntry.getKey();
          Watch watch = watchEntry.getValue();

          if (watch.getCollection().equals(collection)
                  && watch.getId().equals(id)
                  && op.getType() != DocOp.Type.GET
                  && (watch.getPath().startsWith(op.getPath())
                  || op.getPath().startsWith(watch.getPath()))
                  ) {
            watchersToBeCalled.put(watchId, watch);
          }
        });
      });
    });

    logger.info("Invoke {} watchers {}",
            watchersToBeCalled.size(),
            String.join(", ", watchersToBeCalled.keySet().stream().collect(Collectors.toList())));
    watchersToBeCalled.forEachEntry(5, (watchEntry) -> {
      Watch watch = watchEntry.getValue();
      try {
        watch.getListener().onChange(
                collection,
                id,
                watch.getPath(),
                DocOp.get(watch.getPath()).apply(doc, Object.class));
      } catch (Exception e) {
        logger.error("Watch listener error", e);
      }
    });
  }

  /**
   * Broadcast watch invocation to all other ZeppelinHub server through PubSub.
   */
  public void broadcastWatchInvoke(String collection, String id, List<DocOp> ops, Map doc) {
    if (!isChangeFeedEnabled(collection)) {
      String traceId = UUID.randomUUID().toString();
      logger.info("Broadcast watch invocation {} to all servers on {}, {}", traceId, collection, id);

      // if published broadcast message is not coming back on subscribing channel, restart pub/sub and re-publish event
      broadcastVerifier.put(
              traceId,
              threadPoolExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                  logger.error("Missing message {} in topic. restart pubsub", traceId);
                  restartPubsub();
                  broadcastVerifier.remove(traceId);
                  broadcastWatchInvoke(collection, id, ops, doc); // publish event again
                }
              }, pubsubRestartTimeoutSec, TimeUnit.SECONDS));
      topic.publish(new InvokeWatchersBroadcastMessage(traceId, collection, id, ops, doc));
    }
  }

  /**
   * Broadcast doc changes to all client who watches at least one doc.
   * This is to support dev mode. Not invoked in production.
   */
  public void broadcastDocChangeDevMode(String collection, String id, String action, List<ChangeInfo> changes) {
    if (isChangeFeedEnabled(collection)) {
      // change feed will handle it.
      return;
    }
    sendDocChangeToBrowserDevMode(collection, id, action, changes);
  }

  private boolean isChangeFeedEnabled(String collection) {
    WebstoreStorage storage = storageManager.get(collection);
    return storage != null && storage.changeFeedSupported();
  }

  private void sendDocChangeToBrowserDevMode(String collection, String id, String action, List<ChangeInfo> changes) {
    if (!Webstore.isDevMode()) {
      return;
    }

    watches.forEachKey(1, sessionId -> {
      Session session = sessionManager.getSession(sessionId);

      session.rpcInvoke(
              Const.WEBSTORE_RPC_NAME,
              "onChangeDocDevMode",
              new Object[]{collection, id, action, changes});
    });
  }

  private void listenPubsub() {
    topic = pubsub.createOrGetTopic();
    topic.subscribe(this);
  }

  private void restartPubsub() {
    topic.unsubscribe();
    listenPubsub();
  }

  @Override
  public void onMessage(InvokeWatchersBroadcastMessage msg) {
    logger.info("Watch invocation broadcast message {} received for {}, {}",
            msg.getTraceId(), msg.getCollection(), msg.getId());
    ScheduledFuture future = broadcastVerifier.remove(msg.getTraceId());
    if (future != null) {
      future.cancel(true);
    }
    invokeWatchers(
            msg.getCollection(),
            msg.getId(),
            msg.getOps(),
            msg.getDoc());
  }

  public void setPubsubRestartTimeoutSec(int newTimeoutSec) {
    this.pubsubRestartTimeoutSec = newTimeoutSec;
  }

  @Override
  public void onSessionConnect(Session session) {

  }

  @Override
  public void onSessionDisconnect(Session session) {
    removeWatch(session.getId());
  }

  @Override
  public void onChange(String collection, String docId, Map doc) {
    logger.info("Change feed from collection {}, doc {}", collection, docId);
    DocOp op = DocOp.set(DocOp.ROOT_PATH, doc);

    List<ChangeInfo> changeInfos = new LinkedList<>();
    changeInfos.add(new ChangeInfo(op, null));

    sendDocChangeToBrowserDevMode(
            collection,
            docId,
            "CHANGE_FEED - " + collection,
            changeInfos);

    List<DocOp> ops = new LinkedList<>();
    ops.add(op);

    invokeWatchers(
            collection,
            docId,
            ops,
            doc);
  }
}
