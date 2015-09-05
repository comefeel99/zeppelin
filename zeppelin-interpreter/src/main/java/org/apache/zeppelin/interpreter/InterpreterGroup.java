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

package org.apache.zeppelin.interpreter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.helium.ApplicationLoader;
import org.apache.zeppelin.resource.ResourcePool;

/**
 * InterpreterGroup is list of interpreters in the same group.
 * And unit of interpreter instantiate, restart, bind, unbind.
 */
public class InterpreterGroup extends LinkedList<Interpreter>{
  String id;

  AngularObjectRegistry angularObjectRegistry;
  transient String resourcePoolId;
  transient ResourcePool resourcePool;         // for locally loaded interpreter. Not used for
                                               // interpreter loaded in remote process
  transient ApplicationLoader appLoader;       // for locally loaded interpreter.
  public static final transient Map<String, InterpreterGroup> allInterpreterGroups =
      new HashMap<String, InterpreterGroup>(); // all interpreter groups


  public InterpreterGroup(String id) {
    this.id = id;

    synchronized (allInterpreterGroups) {
      allInterpreterGroups.put(id, this);
    }
  }

  public InterpreterGroup() {
    getId();

    synchronized (allInterpreterGroups) {
      allInterpreterGroups.put(id, this);
    }
  }

  private static String generateId() {
    return "InterpreterGroup_" + System.currentTimeMillis() + "_"
           + new Random().nextInt();
  }

  public String getId() {
    synchronized (this) {
      if (id == null) {
        id = generateId();
      }
      return id;
    }
  }

  public Properties getProperty() {
    Properties p = new Properties();
    for (Interpreter intp : this) {
      p.putAll(intp.getProperty());
    }
    return p;
  }

  public AngularObjectRegistry getAngularObjectRegistry() {
    return angularObjectRegistry;
  }

  public void setAngularObjectRegistry(AngularObjectRegistry angularObjectRegistry) {
    this.angularObjectRegistry = angularObjectRegistry;
  }

  public ResourcePool getResourcePool() {
    return resourcePool;
  }

  public void setResourcePool(ResourcePool resourcePool) {
    this.resourcePool = resourcePool;
    setResourcePoolId(resourcePool.getId());
  }

  public ApplicationLoader getAppLoader() {
    return appLoader;
  }

  public void setAppLoader(ApplicationLoader appLoader) {
    this.appLoader = appLoader;
  }

  public String getResourcePoolId() {
    return resourcePoolId;
  }

  public void setResourcePoolId(String resourcePoolId) {
    this.resourcePoolId = resourcePoolId;
  }

  public void close() {
    synchronized (allInterpreterGroups) {
      allInterpreterGroups.remove(id);
    }
    List<Thread> closeThreads = new LinkedList<Thread>();

    for (final Interpreter intp : this) {
      Thread t = new Thread() {
        public void run() {
          intp.close();
        }
      };

      t.start();
      closeThreads.add(t);
    }

    for (Thread t : closeThreads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        Logger logger = Logger.getLogger(InterpreterGroup.class);
        logger.error("Can't close interpreter", e);
      }
    }
  }

  public void destroy() {
    List<Thread> destroyThreads = new LinkedList<Thread>();

    for (final Interpreter intp : this) {
      Thread t = new Thread() {
        public void run() {
          intp.destroy();
        }
      };

      t.start();
      destroyThreads.add(t);
    }

    for (Thread t : destroyThreads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        Logger logger = Logger.getLogger(InterpreterGroup.class);
        logger.error("Can't close interpreter", e);
      }
    }
  }
}
