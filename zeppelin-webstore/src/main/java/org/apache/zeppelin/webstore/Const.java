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

public class Const {
  public static final String ROOT_PATH = "$";
  public static final String WEBSTORE_RPC_NAME = "webstore";

  // when published message is not broadcasted back in this timeout,
  // pubsub will be restarted and message will be re-published.
  public static final int WEBSTORE_WATCH_PUBSUB_TIMEOUT_SEC = 10;

  public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.S'Z";
}
