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
package org.apache.zeppelin.notebook.ipynb;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.notebook.Note;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Ipython notebook format cell
 */
public class Cell {
  public static enum CellType {
    markdown,
    code
  }

  public static enum OutputType {
    display_data,
    execute_result
  }

  public int execution_count;
  public CellType cell_type;
  public Map<String, Object> metadata;

  public List<Output> outputs;
  public OutputType output_type;  // display_data, execute_result

  public List<String> source;

}
