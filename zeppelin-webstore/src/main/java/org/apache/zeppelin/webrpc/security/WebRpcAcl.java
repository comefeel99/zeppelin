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
package org.apache.zeppelin.webrpc.security;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebRpcAcl {
  private static final Logger logger = LoggerFactory.getLogger(WebRpcAcl.class);
  private ConcurrentLinkedQueue<WebRpcAccessRule> rules = new ConcurrentLinkedQueue<>();

  public void allow(WebRpcAccessRule rule) {
    rules.add(rule);
  }

  public void check(AclContext aclContext, String rpcName, String methodName, Class[] types, Object[] params,
                    Object rpcObject, boolean permanentObject)
          throws WebRpcPermissionDeniedException {
    Iterator<WebRpcAccessRule> it = rules.iterator();
    while (it.hasNext()) {
      WebRpcAccessRule rule = it.next();
      try {
        if (rule.check(aclContext, rpcName, methodName, types, params, rpcObject, permanentObject)) {
          // rule allow the operation. Don't have to evaluate other rules.
          return;
        }
      } catch (WebRpcPermissionDeniedException e) {
        // will stop check the rule.
        throw e;
      } catch (Throwable e) {
        // any other exception, we just proceed to the next rule
        logger.error("Exception on rule check", e);
        continue;
      }
    }
    throw new WebRpcPermissionDeniedException("Operation is not allowed by any WebRpcAccessRule");
  }
}
