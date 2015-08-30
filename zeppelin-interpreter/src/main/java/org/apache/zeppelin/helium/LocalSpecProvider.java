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
package org.apache.zeppelin.helium;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Load application Specs from local filesystem
 */
public class LocalSpecProvider {
  Logger logger = LoggerFactory.getLogger(LocalSpecProvider.class);
  private String specDir;

  public LocalSpecProvider(String specDir) {
    this.specDir = specDir;
  }


  public Collection<ApplicationSpec> get() {
    return loadSpecFiles(new File(specDir));
  }


  private Collection<ApplicationSpec> loadSpecFiles(File file) {
    Gson gson = new Gson();
    List<ApplicationSpec> list = new LinkedList<ApplicationSpec>();
    if (!file.exists()) {
      return list;
    }

    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        list.addAll(loadSpecFiles(f));
      }
    } else {
      try {
        FileReader reader = new FileReader(file);
        ApplicationSpec spec = gson.fromJson(reader, ApplicationSpec.class);
        list.add(spec);
      } catch (FileNotFoundException e) {
        logger.error("error", e);
      } catch (Exception e) {
        logger.error("error", e);
      }
    }

    return list;
  }

}
