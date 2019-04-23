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
package org.apache.zeppelin.webstore.security;

import com.jayway.jsonpath.PathNotFoundException;
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webstore.Const;
import org.apache.zeppelin.webstore.DocOp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebstoreAcl {
  private final Logger logger = LoggerFactory.getLogger(WebstoreAcl.class);
  private final ConcurrentLinkedQueue<WebstoreAccessRule> rules = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<WebstoreFindRule> findRules = new ConcurrentLinkedQueue<>();


  public WebstoreAcl() {
  }

  /**
   * Add a rule to check.
   * @param rule
   */
  public void allow(WebstoreAccessRule rule) {
    rules.add(rule);
  }

  public void allowFind(WebstoreFindRule rule) {
    findRules.add(rule);
  }

  /**
   * Check given operation against all defined rules.
   *
   * @param aclContext
   * @param collection collection
   * @param id docId
   * @param op operation to check
   * @param current Doc before this operation applied
   * @param next Doc after this operation applied
   */
  public void check(AclContext aclContext, String collection, String id, DocOp op, Map current, Map next)
          throws WebstorePermissionDeniedException {
    if (rules.isEmpty()) {
      throw new WebstorePermissionDeniedException("Operation is not allowed by any WebRpcAccessRule");
    }

    Iterator<WebstoreAccessRule> it = rules.iterator();
    while (it.hasNext()) {
      WebstoreAccessRule rule = it.next();
      try {
        if (rule.check(aclContext, collection, id, op, current, next)) {
          // rule allow the operation. Don't have to evaluate other rules.
          return;
        }
      } catch (WebstorePermissionDeniedException e) {
        // will stop check the rule.
        throw e;
      } catch (Throwable e) {
        // any other exception, we just proceed to the next rule
        logger.error("Exception on rule check", e);
        continue;
      }
    }
    throw new WebstorePermissionDeniedException("Operation is not allowed by any WebStoreAccessRule");
  }

  public void checkFind(AclContext aclContext, String collection, String startId, String endId)
          throws WebstorePermissionDeniedException {
    if (findRules.isEmpty()) {
      throw new WebstorePermissionDeniedException("Operation is not allowed by any WebStoreAccessRule");
    }

    Iterator<WebstoreFindRule> it = findRules.iterator();
    while (it.hasNext()) {
      WebstoreFindRule rule = it.next();
      try {
        if (rule.check(aclContext, collection, startId, endId)) {
          return;
        }
      } catch (WebstorePermissionDeniedException e) {
        throw e;
      } catch (Throwable e) {
        logger.error("Exception on find rule check", e);
        continue;
      }
    }
  }

  /**
   * Some convenient functions you can use in the rule.
   */
  private Object get(Map doc, String jsonPath, Class type) {
    try {
      return DocOp.get(jsonPath).apply(doc, type);
    } catch (PathNotFoundException e) {
      return null;
    }
  }

  /**
   * Parse jsonPath expression with given list of key label.
   * @param jsonPath
   * @param keys
   * @return
   */
  Map<String, String> parseJsonPath(String jsonPath, String [] keys) throws WebstorePermissionDeniedException {
    Map<String, String> pathElement = new HashMap();
    if (jsonPath.equals(Const.ROOT_PATH)) {
      return pathElement;
    }

    if (!jsonPath.startsWith("$.")) {
      throw new WebstorePermissionDeniedException("Invalid json path " + jsonPath);
    }

    if (jsonPath.indexOf("..") >= 0) {
      throw new WebstorePermissionDeniedException("Invalid string in json path " + jsonPath);
    }

    String[] paths = jsonPath.substring(2).split("[.]");
    if (paths.length != keys.length) {
      throw new WebstorePermissionDeniedException("Json path length not valid " + jsonPath);
    }

    for (int i = 0; i < paths.length; i++) {
      pathElement.put(keys[i], paths[i]);
    }

    return pathElement;
  }

  Map<String, String> parseJsonPath(String jsonPath, Collection<String> keys) throws WebstorePermissionDeniedException {
    return parseJsonPath(jsonPath, keys.toArray(new String[0]));
  }
}
