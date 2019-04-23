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
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.security.WebRpcAcl;
import org.apache.zeppelin.webrpc.transport.memory.MemorySessionManager;
import org.apache.zeppelin.webstore.pubsub.Pubsub;
import org.apache.zeppelin.webstore.pubsub.memory.MemoryPubsub;
import org.apache.zeppelin.webstore.security.WebstoreAccessRule;
import org.apache.zeppelin.webstore.security.WebstoreAcl;
import org.apache.zeppelin.webstore.security.WebstoreFindRule;
import org.apache.zeppelin.webstore.security.WebstorePermissionDeniedException;
import org.apache.zeppelin.webstore.storage.UpdateConflictException;
import org.apache.zeppelin.webstore.storage.WebstoreStorageManager;
import org.apache.zeppelin.webstore.storage.memory.MemoryStorage;
import org.apache.zeppelin.webstore.watch.Watch;
import org.apache.zeppelin.webstore.watch.WatchListener;
import org.apache.zeppelin.webstore.watch.WebstoreWatch;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WebstoreTest {
  private Webstore webstore;
  private WebRpcServer webrpc;
  private MemorySessionManager sessionManager;
  private WebstoreAcl webstoreAcl;
  private Pubsub pubsub;
  private WebstoreStorageManager storageManager;
  private MemoryStorage memoryStorage;

  static int redisPort = 16380;

  WebstoreAccessRule allowAll = new WebstoreAccessRule() {
    @Override
    public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
      return true;
    }
  };
  WebstoreFindRule allowFindAll = new WebstoreFindRule() {
    @Override
    public boolean check(AclContext ctx, String collection, String startDocId, String endDocId) throws WebstorePermissionDeniedException {
      return true;
    }
  };

  @BeforeClass
  public static void init() throws IOException, URISyntaxException {
  }

  @AfterClass
  public static void cleanUp() throws InterruptedException {
  }

  @Before
  public void setUp() {
    WebRpcAcl webRpcAcl = new WebRpcAcl();
    sessionManager = new MemorySessionManager();
    webrpc = new WebRpcServer(sessionManager, webRpcAcl);
    webstoreAcl = new WebstoreAcl();
    pubsub = new MemoryPubsub();
    storageManager = new WebstoreStorageManager();
    memoryStorage = new MemoryStorage();
    storageManager.add(".*", memoryStorage);
    webstore = new Webstore(webrpc, webstoreAcl, pubsub, storageManager);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testDocumentCRUD() throws WebstorePermissionDeniedException, UpdateConflictException {
    webstoreAcl.allow(allowAll);
    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    assertEquals(false, df.exists());

    // create
    assertEquals(null, df.apply(new DocOpBuilder()
            .add(DocOp.set("$.name", "orange"))
            .add(DocOp.set("$.value", 10.0))
            .add(DocOp.set("$.groups.group1.key", "gg"))
            .build()));
    assertEquals(true, df.exists());
    assertEquals("orange", df.get().get("name"));
    assertEquals(10.0, df.get().get("value"));
    assertEquals("gg", df.get("$.groups.group1.key", String.class));

    // update
    assertEquals("orange", df.set("$.name", "apple"));
    assertEquals(10.0, df.set("$.value", 20.0));
    assertEquals("gg", df.set("$.groups.group1.key", "kk"));
    assertEquals("apple", df.get().get("name"));
    assertEquals(20.0, df.get().get("value"));
    assertEquals("kk", df.get("$.groups.group1.key", String.class));

    // move not existing object
    assertEquals(null, df.move("$.nonexists", "$.name"));
    assertEquals("apple", df.get().get("name"));
    // move existing object
    assertEquals(null, df.move("$.name", "$.newName"));
    assertEquals(null, df.get().get("name"));
    assertEquals("apple", df.get().get("newName"));
    // replace existing object
    df.set("$.name", "orange");
    assertEquals("orange", df.move("$.newName", "$.name"));
    assertEquals("apple", df.get().get("name"));
    assertEquals(null, df.get().get("newName"));

    // delete
    assertEquals("apple", df.delete("$.name"));
    assertEquals(null, df.get().get("name"));
    assertEquals(20.0, df.get().get("value"));
    assertEquals("kk", df.get("$.groups.group1.key", String.class));

    // delete root delete document
    df.delete("$");
    assertEquals(false, df.exists());
  }


  @Test
  public void testDeletePathFromNotExistsDoc() throws WebstorePermissionDeniedException, UpdateConflictException {
    // given
    webstoreAcl.allow(allowAll);
    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    assertEquals(false, df.exists());

    // when
    assertEquals(null, df.delete("$.name"));

    // then
    assertEquals(false, df.exists());
  }


  @Test
  public void testDeleteMakeDocumentEmpty() throws WebstorePermissionDeniedException, UpdateConflictException {
    // given
    webstoreAcl.allow(allowAll);
    DocRef df = webstore.getRef(new AclContext(), "col1", "doc2");
    assertEquals(false, df.exists());
    df.set("$.name", "moon");
    assertEquals(true, df.exists());

    // when
    df.delete("$.name");

    // then
    assertEquals(false, df.exists());
  }


  @Test
  public void testACL() throws WebstorePermissionDeniedException, UpdateConflictException {
    AtomicInteger firstRuleCheckCount = new AtomicInteger(0);
    webstoreAcl.allow(new WebstoreAccessRule() {
      @Override
      public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
        firstRuleCheckCount.incrementAndGet();
        if (op.getPath().equals("$.forbidden")) {
          throw new WebstorePermissionDeniedException("forbidden");
        }
        return op.getPath().startsWith("$.user");
      }
    });

    AtomicInteger secondRuleCheckCount = new AtomicInteger(0);
    webstoreAcl.allow(new WebstoreAccessRule() {
      @Override
      public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
        secondRuleCheckCount.incrementAndGet();
        return op.getPath().startsWith("$.name");
      }
    });
    webstoreAcl.allow(new WebstoreAccessRule() {
      @Override
      public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
        return op.getType() == DocOp.Type.DELETE;
      }
    });

    firstRuleCheckCount.set(0);
    secondRuleCheckCount.set(0);

    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    df.set("$.name", "apple");

    assertEquals(1, firstRuleCheckCount.get());
    assertEquals(1, secondRuleCheckCount.get());

    df.get("$.user", Object.class);
    assertEquals(2, firstRuleCheckCount.get());
    assertEquals(1, secondRuleCheckCount.get());
    df.delete();
  }

  @Test(expected = WebstorePermissionDeniedException.class)
  public void testACLThrowException() throws WebstorePermissionDeniedException, UpdateConflictException {
    webstoreAcl.allow(new WebstoreAccessRule() {
      @Override
      public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
        if (op.getPath().equals("$.hidden")) {
          throw new WebstorePermissionDeniedException("forbidden");
        }
        return true;
      }
    });

    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    df.set("$.hidden", "apple");
  }

  @Test
  public void testWatchChange() throws WebstorePermissionDeniedException, InterruptedException, UpdateConflictException {
    webstoreAcl.allow(allowAll);
    WebstoreWatch webstoreWatch = webstore.getWebstoreWatch();
    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    df.set("$.user.name", "moon");
    df.set("$.user.size", 3);

    Thread.sleep(1000); // give enough time to watch broadcast

    AtomicInteger watcher1 = new AtomicInteger(0);
    AtomicInteger watcher2 = new AtomicInteger(0);

    webstoreWatch.addWatch(new AclContext(), "sessionId", "watcher1",
            new Watch("col1", "doc1", "$.user.name", new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
                watcher1.incrementAndGet();
              }
            }));

    webstoreWatch.addWatch(new AclContext(), "sessionId", "watcher2",
            new Watch("col1", "doc1", "$.user", new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
                watcher2.incrementAndGet();
              }
            }));

    df.set("$.user.name", "sun");
    for(int i = 0; i < 10; i++) { Thread.sleep(200); if (watcher1.get() > 0) break;}
    assertEquals(1, watcher1.get());
    assertEquals(1, watcher2.get());

    df.set("$.user.size", 100);
    for(int i = 0; i < 10; i++) { Thread.sleep(200); if (watcher2.get() > 1) break;}
    assertEquals(1, watcher1.get());
    assertEquals(2, watcher2.get());

    webstoreWatch.removeWatch("sessionId", "watcher2");
    df.set("$.user.name", "moon");
    for(int i = 0; i < 10; i++) { Thread.sleep(200); if (watcher1.get() > 1) break;}

    assertEquals(2, watcher1.get());
    assertEquals(2, watcher2.get());

    webstoreWatch.removeWatch("sessionId");
    df.set("$.user.name", "saturn");
    Thread.sleep(1000); // give enough time.

    assertEquals(2, watcher1.get());
    assertEquals(2, watcher2.get());

    df.delete();
  }

  @Test(expected = WebstorePermissionDeniedException.class)
  public void testWatchPermission() throws WebstorePermissionDeniedException, UpdateConflictException {
    webstoreAcl.allow(new WebstoreAccessRule() {
      @Override
      public boolean check(AclContext ctx, String collection, String docId, DocOp op, Map before, Map after) throws WebstorePermissionDeniedException {
        return op.getType() == DocOp.Type.DELETE || op.getPath().startsWith("$.user");
      }
    });
    DocRef df = webstore.getRef(new AclContext(), "col1", "doc1");
    df.set("$.user.name", "moon");

    webstore.getWebstoreWatch().addWatch(new AclContext(), "sessionId", "watcher1",
            new Watch("col1", "doc1", "$.notallowed", new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
              }
            }));
    df.delete();
  }


  @Test
  public void testMultiServerWatchBroadcast() throws WebstorePermissionDeniedException, InterruptedException, UpdateConflictException {
    // create new webstore instance
    MemorySessionManager sessionManager2 = new MemorySessionManager();
    WebRpcServer webrpc2 = new WebRpcServer(sessionManager2, new WebRpcAcl());

    WebstoreStorageManager storageManager2 = new WebstoreStorageManager();
    storageManager2.add(".*", memoryStorage);
    Webstore webstore2 = new Webstore(webrpc2, webstoreAcl, pubsub, storageManager2);

    webstoreAcl.allow(allowAll);

    // watch document from datastore2
    AtomicInteger watchCount2 = new AtomicInteger(0);
    webstore2.getWebstoreWatch().addWatch(new AclContext(), "sessionId", "watcher1",
            new Watch("col1", "doc1", "$.user", new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
                watchCount2.incrementAndGet();
              }
            }));

    // watch document from datastore1
    AtomicInteger watchCount1 = new AtomicInteger(0);
    webstore.getWebstoreWatch().addWatch(new AclContext(), "sessionId", "watcher1",
            new Watch("col1", "doc1", "$.user", new WatchListener() {
              @Override
              public void onChange(String collection, String docId, String path, Object jsonObject) {
                watchCount1.incrementAndGet();
              }
            }));

    // update document from datastore
    DocRef data = webstore.getRef(new AclContext(), "col1", "doc1");
    assertEquals(0, watchCount2.get());
    data.set("$.user.name", "moon");

    for(int i = 0; i < 10; i++) {
      Thread.sleep(200);
      if (watchCount2.get() > 0) break;
    }
    assertEquals(1, watchCount1.get());
    assertEquals(1, watchCount2.get());
    data.delete();
  }

  @Test
  public void testFindRedis() throws WebstorePermissionDeniedException, UpdateConflictException {
    webstoreAcl.allow(allowAll);
    webstoreAcl.allowFind(allowFindAll);
    AclContext aclContext = new AclContext();
    webstore.getRef(aclContext, "col1", "doc10").set("$.name", "doc10");
    webstore.getRef(aclContext, "col1", "doc20").set("$.name", "doc20");
    webstore.getRef(aclContext, "col1", "doc30").set("$.name", "doc30");
    webstore.getRef(aclContext, "col1", "doc40").set("$.name", "doc40");

    Collection<DocRef> docRefs = webstore.findRef(aclContext, "col1", "doc15", "doc30");
    assertEquals(2, docRefs.size());

    Iterator<DocRef> it = docRefs.iterator();
    assertEquals("doc20", it.next().get("$.name", String.class));
    assertEquals("doc30", it.next().get("$.name", String.class));

    docRefs.stream().forEach(doc -> {
      try {
        doc.delete();
      } catch (WebstorePermissionDeniedException e) {
      } catch (UpdateConflictException e) {
        e.printStackTrace();
      }
    });

    assertEquals(0, webstore.findRef(aclContext, "col1", "doc15", "doc30").size());
  }
}
