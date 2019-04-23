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

import org.apache.zeppelin.webstore.security.WebstoreAcl;
import org.apache.zeppelin.webstore.storage.UpdateConflictException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webstore.security.WebstorePermissionDeniedException;
import org.apache.zeppelin.webstore.storage.WebstoreNotFoundException;
import org.apache.zeppelin.webstore.storage.WebstoreStorage;
import org.apache.zeppelin.webstore.watch.ChangeInfo;
import org.apache.zeppelin.webstore.watch.WebstoreWatch;

import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference to a document in webstore.
 * This class helps read/update/delete target document.
 *
 * Currently this class implemented for CouchDB.
 * While this class uses very basic CRUD api of CouchDB, it'll not be
 * very difficult to migrate to other storage system in the future.
 * Any document storage can store json object would work.
 */
public class DocRef {
  Logger logger = LoggerFactory.getLogger(DocRef.class);
  private static final int MAX_SET_TRY = 100;

  private final String collection;
  private final String id;
  private final WebstoreAcl webstoreAcl;
  private final AclContext aclCtx;
  private final WebstoreWatch webstoreWatch;
  private final WebstoreStorage db;

  DocRef(String collection, String id, WebstoreAcl webstoreAcl, AclContext aclCtx,
         WebstoreWatch webstoreWatch, WebstoreStorage db) {
    this.collection = collection;
    this.id = id;
    this.webstoreAcl = webstoreAcl;
    this.aclCtx = aclCtx;
    this.webstoreWatch = webstoreWatch;
    this.db = db;
  }

  /**
   * Get the name of Collection that this document belongs to.
   * @return
   */
  public String collection() {
    return collection;
  }

  /**
   * Get document Id in the Collection.
   * @return
   */
  public String id() {
    return id;
  }

  /**
   * Apply list of DocOp to json document this class points to.
   * DocOp may add, update, delete keys, values to the json documents.
   *
   * You can update json document using set() delete() method in this class.
   * Difference between set(), delete() and apply() is, apply() able to modify
   * multiple updates, delete nested json object atomically while multiple set() update()
   * call does not guarantees atomic update.
   *
   * All the DocOp in the list will be either applied or not applied at the same time.
   * Once all DocOp are applied, updated json document will is persisted in the CouchDb.
   *
   * On persisting document, there can be a conflict, while CouchDb uses optimistic lock.
   * When conflict detected, this method simply retry
   *   - GET json document
   *   - Apply DocOp
   *   - Persist
   * until it success.
   *
   * @param ops
   * @param actionName This parameter does not affect actual operation. Give name for debugging purpose.
   * @return
   */
  public Object apply(List<DocOp> ops, String actionName) throws WebstorePermissionDeniedException, UpdateConflictException {
    for (int i = 0; i < MAX_SET_TRY; i++) {
      try {
        Object ret = null;
        Map doc = null;
        boolean updated = false;
        List<ChangeInfo> changes = new LinkedList<>();
        boolean docExists;

        try {
          doc = db.get(collection, id);
          docExists = true;
        } catch (WebstoreNotFoundException e) {
          docExists = false;
        }

        if (doc == null) {
          doc = new HashMap();
        }
        Map orignalDoc = doc;

        Iterator<DocOp> it = ops.iterator();
        while (it.hasNext()) {
          DocOp op = it.next();
          Map before = doc;
          if (op.getType() == DocOp.Type.GET) {
            // GET operation does not have any effect here
          } else { // apply SET, DELETE operation on doc
            Object previousValue;
            if (DocOp.ROOT_PATH.equals(op.getPath())) {
              previousValue = doc;
              Map empty = new HashMap<>();
              // couchdb specific code. Couchdb require _rev and _id field to update
              if (doc.containsKey("_rev")) {
                empty.put("_rev", doc.get("_rev"));
              }
              if (doc.containsKey("_id")) {
                empty.put("_id", doc.get("_id"));
              }
              doc = empty;
            } else {
              doc = (Map) op.apply(doc, Map.class);
              previousValue = doc;
            }
            updated = true;

            if (Webstore.isDevMode()) {
              changes.add(new ChangeInfo(op, previousValue));
            }
          }

          // check ACL
          webstoreAcl.check(aclCtx, collection, id, op, before, doc);

          // if it is last operation, return value
          if (!it.hasNext()) {
            try {
              if (op.getType() == DocOp.Type.GET) {
                ret = op.apply(doc, Object.class);
              } else if (op.getType() == DocOp.Type.SET) {
                ret = DocOp.get(op.getPath()).apply(before, Object.class);
              } else if (op.getType() == DocOp.Type.SET_DATE) {
                ret = DocOp.get(op.getPath()).apply(before, Object.class);
              } else if (op.getType() == DocOp.Type.MOVE) {
                if (null == DocOp.get(op.getPath()).apply(before, Object.class)) {
                  ret = null;
                } else {
                  ret = DocOp.get((String) op.getValue()).apply(before, Object.class);
                }
              } else if (op.getType() == DocOp.Type.DELETE) {
                if (DocOp.ROOT_PATH.equals(op.getPath())) {
                  if (docExists) {
                    db.delete(collection, id);
                    return doc;
                  } else {
                    return null;
                  }
                } else {
                  if (docExists) {
                    ret = DocOp.get(op.getPath()).apply(before, Object.class);
                  } else {
                    ret = null;
                  }
                }
              }
            } catch (PathNotFoundException e) {
              ret = null;
            }
          }
        }


        if (updated) {
          if (docExists) {
            if (doc != null && doc.size() > 0) {
              db.update(collection, id, doc); // update
            } else {
              db.delete(collection, id);      // delete
            }
          } else {
            if (doc != null && doc.size() > 0) {
              db.update(collection, id, doc); // update
            } else {
              // nothing to do
            }
          }

          // send state change to client watching this document
          if (Webstore.isDevMode()) {
            // add entire doc change info to the change list.
            changes.add(new ChangeInfo(DocOp.set(DocOp.ROOT_PATH, doc), orignalDoc));
            webstoreWatch.broadcastDocChangeDevMode(collection, id, actionName, changes);
          }

          webstoreWatch.broadcastWatchInvoke(collection, id, ops, doc);
        }
        return ret;
      } catch (UpdateConflictException e) {
        // document conflict. retry
        if (i == MAX_SET_TRY - 1) {
          throw e;
        } else {
          continue;
        }
      }
    }

    return null; // won't happen
  }

  public Object apply(List<DocOp> ops) throws WebstorePermissionDeniedException, UpdateConflictException {
    return apply(ops, "apply");
  }

  /**
   * Get json object this class references.
   * @return Map object that represents the json document.
   */
  public Map get() throws WebstorePermissionDeniedException {
    try {
      Map doc = db.get(collection, id);
      webstoreAcl.check(aclCtx, collection, id, DocOp.get(DocOp.ROOT_PATH), doc, doc);
      return doc;
    } catch (WebstoreNotFoundException e) {
      return null;
    }
  }

  /**
   * Get nested json object using JsonPath.
   * Json path is something like
   *
   * $.chatroom.title
   *
   * It's like a Xpath for XML.
   * To learn more about JsonPath, see https://github.com/json-path/JsonPath
   *
   * @param jsonPath json path string
   * @param type return type
   * @return json object in jsonPath
   */
  public Object get(String jsonPath, Class type) throws WebstorePermissionDeniedException {
    try {
      Map doc = db.get(collection, id);
      DocOp op = DocOp.get(jsonPath);
      webstoreAcl.check(aclCtx, collection, id, op, doc, doc);
      return op.apply(doc, type);
    } catch (WebstoreNotFoundException e) {
      return null;
    } catch (PathNotFoundException e) {
      return null;
    }
  }

  public Object get(String jsonPath) throws WebstorePermissionDeniedException {
    return get(jsonPath, Object.class);
  }

  /**
   * Store jsonObject in the given jsonPath.
   * If jsonPath does not exists, it automatically creates including parents. (like mkdir -p)
   *
   * @param jsonPath JsonPath string
   * @param jsonObject Json object to add
   * @return Nested object being replaced
   */
  public Object set(String jsonPath, Object jsonObject) throws WebstorePermissionDeniedException, UpdateConflictException {
    return apply(new DocOpBuilder().add(DocOp.set(jsonPath, jsonObject)).build(), "SET");
  }

  public Object move(String fromJsonPath, String toJsonPath) throws WebstorePermissionDeniedException, UpdateConflictException {
    return apply(new DocOpBuilder().add(DocOp.move(fromJsonPath, toJsonPath)).build(), "MOVE");
  }

  /**
   * Delete nested json object in the given jsonPath.
   * @param jsonPath JsonPath string
   * @return Nested object being deleted
   */
  public Object delete(String jsonPath) throws WebstorePermissionDeniedException, UpdateConflictException {
    return apply(new DocOpBuilder().add(DocOp.delete(jsonPath)).build(), "DELETE");
  }

  /**
   * Delete this json document from couchdb.
   * @return entire object being deleted.
   */
  public Object delete() throws WebstorePermissionDeniedException, UpdateConflictException {
    return delete(Const.ROOT_PATH);
  }

  /**
   * Check existance of this json document in the couchdb.
   * @return
   */
  public boolean exists() {
    return db.exists(collection, id);
  }
}
