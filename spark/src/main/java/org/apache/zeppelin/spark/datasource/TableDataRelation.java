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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.TaskScheduler;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.zeppelin.tabledata.ColumnDef;
import org.apache.zeppelin.tabledata.TableData;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Tabledata relation
 */
public class TableDataRelation extends BaseRelation implements Serializable, TableScan {
  transient SQLContext context;
  private final TableData data;

  public TableDataRelation(SQLContext context, TableData data) {
    this.context = context;
    this.data = data;
  }

  @Override
  public SQLContext sqlContext() {
    return context;
  }

  @Override
  public StructType schema() {
    ColumnDef[] columns = data.columns();
    StructField [] fields = new StructField[columns.length];
    int i = 0;
    for (ColumnDef c : columns) {
      if (c.type() == ColumnDef.TYPE.INT) {
        fields[i] = new StructField(c.name(), IntegerType, true, Metadata.empty());
      } else if (c.type() == ColumnDef.TYPE.LONG) {
        fields[i] = new StructField(c.name(), LongType, true, Metadata.empty());
      } else {
        fields[i] = new StructField(c.name(), StringType, true, Metadata.empty());
      }
      i++;
    }
    return new StructType(fields);
  }

  @Override
  public RDD<Row> buildScan() {
    Iterator<org.apache.zeppelin.tabledata.Row> rows = data.rows();
    List<org.apache.zeppelin.tabledata.Row> result = new ArrayList();
    while (rows.hasNext()){
      result.add(rows.next());
    }

    JavaSparkContext jsc = new JavaSparkContext(context.sparkContext());
    JavaRDD<org.apache.zeppelin.tabledata.Row> rdd = jsc.parallelize(result);
    return rdd.map(new Function<org.apache.zeppelin.tabledata.Row, Row>() {
      @Override
      public Row call(org.apache.zeppelin.tabledata.Row row) throws Exception {
        return org.apache.spark.sql.RowFactory.create(row.get());
      }
    }).rdd();
  }
}
