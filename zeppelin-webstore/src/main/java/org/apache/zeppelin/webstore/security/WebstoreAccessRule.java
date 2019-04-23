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

import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webstore.DocOp;
import java.util.Map;

public interface WebstoreAccessRule {
  /**
   * check() method returns true if given DocOp is allowed.
   * If check() method returns false, WebstoreACL will proceed to check the next rule.
   *
   * However, if check() method throws WebstorePermissionDeniedException,
   * WebstoreACL will not proceed to next rule and disallow given DocOp immediately.
   *
   * @param ctx
   * @param collection
   * @param docId
   * @param op
   * @param before
   * @param after
   * @return
   * @throws WebstorePermissionDeniedException
   */
  boolean check(
          AclContext ctx,
          String collection,
          String docId,
          DocOp op,
          Map before,
          Map after
  ) throws WebstorePermissionDeniedException;
}
