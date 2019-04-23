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
package org.apache.zeppelin.webstore.storage;

import java.util.Collection;
import java.util.Map;

/**
 * Webstore storage interface.
 */
public interface WebstoreStorage {

  /**
   * Create or update document in the db.
   * @param collection
   * @param docId
   * @param doc
   */
  void update(String collection, String docId, Object doc) throws UpdateConflictException;

  /**
   * Get document
   * @param collection
   * @param docId
   * @return
   */
  Map get(String collection, String docId) throws WebstoreNotFoundException;

  /**
   * Delete document.
   * @param collection
   * @param docId
   */
  void delete(String collection, String docId);

  /**
   * Check document existance.
   * @param collection
   * @param docId
   * @return
   */
  boolean exists(String collection, String docId);

  /**
   * find documents in given range by docId (inclusive).
   * @param collection
   * @param startDocId
   * @param endDocId
   * @return
   */
  Collection<String> find(String collection, String startDocId, String endDocId);

  /**
   * Whether this storage support change feed or not
   * @return
   */
  boolean changeFeedSupported();

  /**
   * Set change feed listener
   * @param listener
   */
  void setChangeFeedListener(ChangeFeedListener listener);
}
