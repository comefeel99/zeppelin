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
package org.apache.zeppelin.resource;

/**
 * Well known resource names (patterns)
 */
public enum WellKnownResource {
  APPLICATION("org.apache.zeppelin.helium.Application"),
  TABLE_DATA("org.apache.zeppelin.interpreter.data.TableData"),
  /*
   * Spark interpreter provided resource
   */
  SPARK_CONTEXT("org.apache.spark.SparkContext"),
  SPARK_SQLCONTEXT("org.apache.spark.sql.SQLContext");


  String type;
  WellKnownResource(String type) {
    this.type = type;
  }

  public String type() {
    return type;
  }

  public static String resourceName(
      WellKnownResource res, String instanceId, String noteId, String praragraphId) {
    return resourceName(res.type(), instanceId, noteId, praragraphId);
  }

  public static final String INSTANCE_ALL = "[^@]*";
  public static final String INSTANCE_RESULT = "result";

  public static String resourceName(
      String type, String instanceId, String noteId, String praragraphId) {
    return type + "#" + instanceId + "@" + noteId + ":" + praragraphId;
  }

  public static String resourceNameBelongsTo(
      String noteId, String praragraphId) {
    return "[^@]*[@]" + noteId + ":" + praragraphId;
  }
}
