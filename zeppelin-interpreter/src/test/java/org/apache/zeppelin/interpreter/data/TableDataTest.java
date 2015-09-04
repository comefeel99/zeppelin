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

import static org.junit.Assert.*;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.Test;

public class TableDataTest {

  @Test
  public void testTableData() {
    InterpreterResult result = new InterpreterResult(Code.SUCCESS, "%table key\tvalue\na\t1\nb\t2");
    TableData td = new TableData(result);
    assertEquals(2, td.getColumnDef().length);
    assertEquals("key", td.getColumnDef()[0].getName());
    assertEquals("value", td.getColumnDef()[1].getName());
    assertEquals(2, td.length());
    assertEquals(2, td.getColumn(0).length);
    assertEquals("a", td.getData(0, 0));
    assertEquals("2", td.getData(1, 1));
  }

}
