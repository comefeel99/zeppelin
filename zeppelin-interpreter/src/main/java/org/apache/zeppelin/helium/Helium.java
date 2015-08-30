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

import java.util.Collection;

/**
 * Helium
 *
 */
public class Helium {
  HeliumConf conf;
  private LocalSpecProvider localSpecProvider;
  private ApplicationLoader appLoader;

  public Helium(HeliumConf conf, String localSpecDir, ApplicationLoader appLoader) {
    this.conf = conf;
    localSpecProvider = new LocalSpecProvider(localSpecDir);
    this.appLoader = appLoader;
  }

  public Collection<ApplicationSpec> getAllSpecs() {
    Collection<ApplicationSpec> localSpecs = localSpecProvider.get();

    for (ApplicationSpec s : localSpecs) {
      if (conf.isEnabled(s)) {
        s.setEnabled(true);
      }
    }

    return localSpecs;
  }

  public HeliumConf getConf() {
    return conf;
  }


}
