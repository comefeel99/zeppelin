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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.remote.InterpreterConnectionFactory;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.thrift.ApplicationResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.resource.WellKnownResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helium
 *
 */
public class Helium {
  Logger logger = LoggerFactory.getLogger(Helium.class);
  static Helium singleton_instance = null;
  HeliumConf conf;
  private LocalSpecProvider localSpecProvider;

  public Helium(HeliumConf conf, String localSpecDir) {
    this.conf = conf;
    localSpecProvider = new LocalSpecProvider(localSpecDir);
  }

  public static void setSingleton(Helium helium) {
    Helium.singleton_instance = helium;
  }

  public static Helium singleton() {
    return singleton_instance;
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



  /**
   * Get all possible applications that can consume specified resource
   * @param resourceId
   * @return
   */
  public Collection<ApplicationSpec> getAllApplicationsForResource(String resourceId) {
    List<ApplicationSpec> possibleSpecs = new LinkedList<ApplicationSpec>();

    Collection<ApplicationSpec> specs = getAllSpecs();
    for (ApplicationSpec spec : specs) {
      if (canConsume(spec, resourceId)) {
        possibleSpecs.add(spec);
      }
    }

    return possibleSpecs;
  }

  private boolean canConsume(ApplicationSpec spec, String resourceId) {
    String[] consumes = spec.getConsume();
    if (consumes == null) {
      return false;
    }

    for (String c : consumes) {
      if (resourceId.startsWith(c)) {
        return true;
      }

      if (Pattern.matches(c, resourceId)) {
        return true;
      }
    }

    return false;
  }
  public void unload(ApplicationKey key, String location, String noteId, String paragraphId)
      throws ApplicationException {
    for (InterpreterGroup intpGroup : getAllInterpreterGroups()) {
      String poolId = intpGroup.getResourcePoolId();

      // If resourcePoolId is not set, ask interpreter process and set
      if (poolId == null) {
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

        poolId = getResourcePoolId(cf);

        if (poolId != null) {
          intpGroup.setResourcePoolId(poolId);
        }
      }

      if (poolId == null || !poolId.equals(location)) {
        continue;
      }
      if (intpGroup.getResourcePool() != null) {   // local interpreter process
        ResourcePool pool = intpGroup.getResourcePool();
        Object app = pool.get(WellKnownResource.resourceName(
            WellKnownResource.APPLICATION,
            paragraphId,
            noteId,
            paragraphId));

        if (app != null) {
          try {
            ((Application) app).unload();
          } catch (IOException e) {
            throw new ApplicationException(e);
          }
        }
        return;
      } else {
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

        int ret = 0;
        try {
          ret = c.unloadApplication(
              key.getMavenArtifact(),
              key.getClassName(),
              noteId,
              paragraphId);
        } catch (TException e) {
          logger.error("error", e);
        } finally {
          cf.releaseClient(c);
        }

        if (ret != 0) {
          throw new ApplicationException("Application unload error " + key.getClassName());
        } else {
          return;
        }
      }
    }
  }

  private List<InterpreterGroup> getAllInterpreterGroups() {
    List<InterpreterGroup> all = new LinkedList<InterpreterGroup>();
    synchronized (InterpreterGroup.allInterpreterGroups) {
      all.addAll(InterpreterGroup.allInterpreterGroups.values());
    }
    return all;
  }

  public void load(ApplicationKey key, String location, String noteId, String paragraphId,
      ApplicationArgument arg,
      InterpreterContext context)
      throws ApplicationException {
    for (InterpreterGroup intpGroup : getAllInterpreterGroups()) {
      String poolId = intpGroup.getResourcePoolId();

      // If resourcePoolId is not set, ask interpreter process and set
      if (poolId == null) {
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

        poolId = getResourcePoolId(cf);

        if (poolId != null) {
          intpGroup.setResourcePoolId(poolId);
        }
      }

      if (poolId == null || !poolId.equals(location)) {
        continue;
      }
      if (intpGroup.getResourcePool() != null) {   // local interpreter process
        String appResourceName = WellKnownResource.resourceName(
            WellKnownResource.APPLICATION,
            paragraphId,
            noteId,
            paragraphId);

        ResourcePool pool = intpGroup.getResourcePool();

        Object app = pool.get(appResourceName);
        if (app == null) {
          Application application = intpGroup.getAppLoader().load(key, context);
          pool.put(WellKnownResource.resourceName(
              WellKnownResource.APPLICATION,
              paragraphId,
              noteId,
              paragraphId), application);
          try {
            application.load();
            app = application;
          } catch (IOException e) {
            throw new ApplicationException(e);
          }
        }

        if (app != null) {
          try {
            ((Application) app).run(arg);
          } catch (IOException e) {
            throw new ApplicationException(e);
          }
        }
        return;
      } else {
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

        ApplicationResult ret = null;
        try {
          ret = c.loadApplication(
              key.getMavenArtifact(),
              key.getClassName(),
              noteId,
              paragraphId,
              arg.getResourceId(),
              RemoteInterpreter.convert(context));
        } catch (TException e) {
          logger.error("error", e);
        } finally {
          cf.releaseClient(c);
        }

        if (ret.code != 0) {
          throw new ApplicationException("Application error " + ret.getOutput());
        } else {
          return;
        }
      }
    }
  }

  private String getResourcePoolId(InterpreterConnectionFactory cf) {
    if (cf == null || !cf.isRunning()) {
      return null;
    }

    Client c;
    try {
      c = cf.getClient();
    } catch (Exception e1) {
      // just ignore the connection
      return null;
    }

    String poolId = null;
    try {
      poolId = c.getResourcePoolId();
    } catch (TException e) {
      logger.error("error", e);
    } finally {
      cf.releaseClient(c);
    }
    return poolId;
  }
}
