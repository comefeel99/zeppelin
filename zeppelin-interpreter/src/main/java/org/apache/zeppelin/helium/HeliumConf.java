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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.Gson;

/**
 * HeliumConf - usually, ZEPPELIN_CONF/helium.json
 * Responsible for save/load enabled ZeppelinApplication
 */
public class HeliumConf {
  List<ApplicationKey> enabled = new LinkedList<ApplicationKey>();
  private transient File confFile;

  HeliumConf() {
  }

  public static HeliumConf create(File confFile) throws IOException {
    // read conf from file
    Gson gson = new Gson();

    HeliumConf hc;

    // if file is not exists, create an empty file
    if (!confFile.exists()) {
      hc = new HeliumConf();
      hc.confFile = confFile;
      hc.save();
    } else {
      FileReader reader = new FileReader(confFile);
      hc = gson.fromJson(reader, HeliumConf.class);
      hc.confFile = confFile;
      reader.close();
    }

    return hc;
  }

  private void save() throws IOException {
    // save conf to file
    Gson gson = new Gson();

    String jsonString = gson.toJson(this);

    FileOutputStream fos = new FileOutputStream(confFile, false);
    OutputStreamWriter out = new OutputStreamWriter(fos);
    out.append(jsonString);
    out.close();
    fos.close();
  }

  public boolean isEnabled(ApplicationKey key) {
    synchronized (enabled) {
      return enabled.contains(key);
    }
  }

  private ApplicationKey clone(ApplicationKey spec) {
    return new ApplicationKey(spec.getMavenArtifact(), spec.getClassName());
  }

  public void enable(ApplicationSpec spec) throws IOException {
    synchronized (enabled) {
      if (!enabled.contains(spec)) {
        enabled.add(clone(spec));
      }
      save();
    }
    spec.setEnabled(true);
  }

  public void disable(ApplicationSpec spec) throws IOException {
    synchronized (enabled) {
      enabled.remove(clone(spec));
      save();
    }
    spec.setEnabled(false);
  }
}
