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
package org.apache.zeppelin.interpreter.data;

import java.io.Serializable;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;

/**
 * Table data representation
 */
public class TableData implements Serializable {
  private final ColumnDef [] columnDef;
  private final Object[][] columnData;

  public TableData(InterpreterResult result) {
    if (Type.TABLE != result.type()) {
      throw new IllegalArgumentException("Supports only table type result");
    }

    String[] rows = result.message().split("\n");
    if (rows == null || rows.length == 0) {
      columnDef = null;
      columnData = null;
    } else {
      String[] headerRow = rows[0].split("\t");
      columnDef = new ColumnDef[headerRow.length];
      columnData = new Object[columnDef.length][];
      for (int i = 0; i < headerRow.length; i++) {
        columnDef[i] = new ColumnDef(headerRow[i]);
        columnData[i] = new Object[rows.length - 1];
      }

      for (int r = 1; r < rows.length; r++) {
        Object [] row = rows[r].split("\t");
        for (int c = 0; c < columnDef.length; c++) {
          if (row.length <= c) {
            columnData[c][r - 1] = null;
          } else {
            columnData[c][r - 1] = row[c];
          }
        }
      }
    }
  }


  public ColumnDef [] getColumnDef() {
    return columnDef;
  }

  public Object [] getColumn(int c) {
    return columnData[c];
  }

  public Object getData(int row, int column) {
    return columnData[column][row];
  }

  public int length() {
    if (columnData == null || columnData.length == 0 || columnData[0] == null) {
      return 0;
    } else {
      return columnData[0].length;
    }
  }
}
