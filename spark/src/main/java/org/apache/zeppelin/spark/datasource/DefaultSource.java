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
package org.apache.zeppelin.spark.datasource;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.resource.*;
import org.apache.zeppelin.tabledata.InterpreterResultTableData;
import org.apache.zeppelin.tabledata.TableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

/**
 * Datasource to read table data from resource pool
 */
public class DefaultSource implements RelationProvider, SchemaRelationProvider {
  Logger logger = LoggerFactory.getLogger(DefaultSource.class);
  public static ResourcePool resourcePool;

  public DefaultSource() {
  }

  @Override
  public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
    return createRelation(sqlContext, parameters, null);
  }

  @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      Map<String, String> parameters,
      StructType schema) {
    String path = parameters.get("path").get();
    String [] noteIdAndParagraphId = path.split("\\|");

    ResourceSet rs = ResourcePoolUtils.getAllResources();
    Resource resource = resourcePool.get(
        noteIdAndParagraphId[0],
        noteIdAndParagraphId[1],
        WellKnownResourceName.ZeppelinTableResult.toString());

    InterpreterResultMessage message = (InterpreterResultMessage) resource.get();
    TableData tableData = new InterpreterResultTableData(message);

    return new TableDataRelation(sqlContext, tableData);
  }

}
