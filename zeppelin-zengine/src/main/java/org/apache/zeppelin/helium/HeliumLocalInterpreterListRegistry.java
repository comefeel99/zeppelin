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

import com.google.gson.Gson;
import org.apache.zeppelin.interpreter.install.InstallInterpreter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Read conf/interpreter-list file and provide helium package information
 */
public class HeliumLocalInterpreterListRegistry extends HeliumRegistry {

  private final Gson gson;

  public HeliumLocalInterpreterListRegistry(String name, String uri) {
    super(name, uri);
    gson = new Gson();
  }

  @Override
  public List<HeliumPackage> getAll() throws IOException {
    InstallInterpreter installer = new InstallInterpreter(
        new File(uri()),
        null,
        null);

    //List<InstallInterpreter.AvailableInterpreterInfo> list = installer.list();
    return null;
  }
}
