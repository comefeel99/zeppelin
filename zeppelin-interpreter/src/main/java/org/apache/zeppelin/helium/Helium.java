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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.remote.InterpreterConnectionFactory;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.thrift.ApplicationResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;

/**
 * Helium
 *
 */
public class Helium {
  HeliumConf conf;
  private LocalSpecProvider localSpecProvider;

  public Helium(HeliumConf conf, String localSpecDir, ApplicationLoader appLoader) {
    this.conf = conf;
    localSpecProvider = new LocalSpecProvider(localSpecDir);
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

  public void runApplication(ApplicationKey key, String location, InterpreterContext context)
      throws ApplicationException {
    List<InterpreterGroup> targetIntpGroups = new LinkedList<InterpreterGroup>();
    synchronized (InterpreterGroup.allInterpreterGroups) {
      for (InterpreterGroup intpGroup : InterpreterGroup.allInterpreterGroups.values()) {
        String poolId = intpGroup.getResourcePoolId();

        if (intpGroup.getResourcePool() != null) {  // local
          intpGroup.getAppLoader().run(key, context);
          return;
        }

        if (poolId == null || poolId.equals(location)) {
          targetIntpGroups.add(intpGroup);
        }
      }

      for (InterpreterGroup intpGroup : targetIntpGroups) {
        // remote interpreter's pool
        if (intpGroup.size() == 0) {
          continue;
        }
        Interpreter anyInterpreter = intpGroup.get(0); // because of all remote interpreter
                                                       // in the same group uses
                                                       // the same resource pool.

        if (!(anyInterpreter instanceof RemoteInterpreter)) {
          continue;
        }

        RemoteInterpreter r = (RemoteInterpreter) anyInterpreter;
        InterpreterConnectionFactory cf = r.getInterpreterConnectionFactory();
        if (cf == null || !cf.isRunning()) {
          continue;
        }

        Client c;
        try {
          c = cf.getClient();
        } catch (Exception e1) {
          // just ignore the connection
          continue;
        }

        try {
          String poolId = intpGroup.getResourcePoolId();
          if (poolId == null) {
            poolId = c.getResourcePoolId();
            intpGroup.setResourcePoolId(poolId);
          }

          if (location.equals(poolId)) {
            ApplicationResult ret = c.runApplication(key.getMavenArtifact(), key.getClassName(),
                RemoteInterpreter.convert(context));

            if (ret.code != 0) {
              throw new ApplicationException(ret.output);
            }
          }
        } catch (TException e) {
          e.printStackTrace();
        } finally {
          cf.releaseClient(c);
        }
      }
    }
  }
}
