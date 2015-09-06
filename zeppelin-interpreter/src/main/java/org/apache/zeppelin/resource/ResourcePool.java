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
package org.apache.zeppelin.resource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.interpreter.remote.InterpreterConnectionFactory;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Resource pool stores resources.
 * Each interpreterGroup (interpreter process) has one.
 * ZeppelinServer has one.
 * They can reference each other
 */
public class ResourcePool {
  Map<ResourceInfo, Object> localPool;
  static final String LOCATION_ANY = "*";
  static final String NAME_ANY = "*";
  private final String id;
  private ResourcePoolEventHandler resourcePoolEventHandler;

  public ResourcePool(ResourcePoolEventHandler resourcePoolEventHandler) {
    id = generatePoolId();
    this.resourcePoolEventHandler = resourcePoolEventHandler;
    localPool = new HashMap<ResourceInfo, Object>();
  }

  public String getId() {
    return id;
  }

  private String generatePoolId() {
    return "resourcePool_" + System.currentTimeMillis() + "_" + new Random().nextInt(1000);
  }

  public void put(String name, Object o) {
    ResourceInfo info = new ResourceInfo(id, name, o);
    synchronized (localPool) {
      localPool.put(info, o);
    }
  }


  private ResourceInfo resourceInfoFromName(String name) {
    return new ResourceInfo(id, name, this);
  }

  /**
   * Get object from pool.
   * @param name
   * @return
   */
  public Object get(String name) {
    return get(LOCATION_ANY, name);
  }

  public Object get(String resourcePoolId, String name) {
    if (resourcePoolId.equals(id)) { // get locally
      synchronized (localPool) {
        return localPool.get(resourceInfoFromName(name));
      }
    } else {
      if (resourcePoolId.equals(LOCATION_ANY)) {
        synchronized (localPool) { // try locally first
          if (localPool.containsKey(resourceInfoFromName(name))) {
            return localPool.get(resourceInfoFromName(name));
          }
        }
      }
      return resourcePoolEventHandler.resourcePoolGetObject(resourcePoolId, name);
    }
  }

  public Collection<ResourceInfo> search(String namePattern) {
    return search(LOCATION_ANY, namePattern);
  }
  public Collection<ResourceInfo> search(String resourcePoolId, String namePattern) {
    List<ResourceInfo> info = new LinkedList<ResourceInfo>();
    if (resourcePoolId.equals(id)) { // search locally
      for (ResourceInfo r :localPool.keySet()) {
        if (match(getId(), namePattern, r)) {
          info.add(r);
        }
      }
      return info;
    } else {
      Collection<ResourceInfo> globalInfo = resourcePoolEventHandler.resourcePoolSearch(
          resourcePoolId, namePattern);
      if (globalInfo == null) {
        // fall back to local search
        return search(id, namePattern);
      } else {
        return globalInfo;
      }
    }
  }

  public static boolean match(String location, String namePattern, ResourceInfo info) {
    if (location.equals(LOCATION_ANY) || info.location().equals(location)) {
      return namePattern.equals(NAME_ANY) || Pattern.matches(namePattern, info.name());
    } else {
      return false;
    }
  }

  public void remove(String name) {
    synchronized (localPool) {
      localPool.remove(resourceInfoFromName(name));
    }
  }

  public static Collection<ResourceInfo> searchAll (String location, String namePattern) {
    Gson gson = new Gson();
    List<ResourceInfo> searchedInfo = new LinkedList<ResourceInfo>();
    List<InterpreterGroup> interpreterGroupToCheck =
        new LinkedList<InterpreterGroup>();

    // search over locally loaded interpreters
    synchronized (InterpreterGroup.allInterpreterGroups) {
      for (InterpreterGroup intpGroup : InterpreterGroup.allInterpreterGroups.values()) {
        // local interpreter pool search
        ResourcePool pool = intpGroup.getResourcePool();

        if (pool != null) {
          if (ResourcePool.LOCATION_ANY.equals(location) || location.equals(pool.getId())) {
            searchedInfo.addAll(pool.search(namePattern));
          }
          continue;
        } else {
          String poolId = intpGroup.getResourcePoolId();
          if (poolId != null &&
              !(ResourcePool.LOCATION_ANY.equals(location) || location.equals(poolId))) {
            continue;
          }
        }

        interpreterGroupToCheck.add(intpGroup);
      }
    }


    for (InterpreterGroup intpGroup : interpreterGroupToCheck) {
      // remote interpreter's pool
      if (intpGroup.size() == 0) {
        continue;
      }
      Interpreter anyInterpreter = intpGroup.get(0); // because of all remote interpreter
                                                     // in the same group uses

      if (anyInterpreter == null) {
        continue;
      }

      while (anyInterpreter instanceof WrappedInterpreter){
        anyInterpreter = ((WrappedInterpreter) anyInterpreter).getInnerInterpreter();
      }

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

        if (ResourcePool.LOCATION_ANY.equals(location) || location.equals(poolId)) {
          Collection<ResourceInfo> infos = gson.fromJson(c.resourcePoolSearch(namePattern),
              new TypeToken<Collection<ResourceInfo>>(){}.getType());

          for (ResourceInfo info : infos) {
            if (ResourcePool.match(location, namePattern, info)) {
              searchedInfo.add(info);
            }
          }
        }
      } catch (TException e) {
        e.printStackTrace();
      } finally {
        cf.releaseClient(c);
      }
    }

    return searchedInfo;
  }


  /**
   *
   * @param location exact location. ResourcePool.LOCATION_ANY is not supported.
   * @param name
   * @return
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public static Object getFromAll (String location, String name)
      throws ClassNotFoundException, IOException {

    List<InterpreterGroup> interpreterGroupToCheck =
        new LinkedList<InterpreterGroup>();

    // search over locally loaded interpreters
    synchronized (InterpreterGroup.allInterpreterGroups) {
      for (InterpreterGroup intpGroup : InterpreterGroup.allInterpreterGroups.values()) {
        // local pool
        ResourcePool pool = intpGroup.getResourcePool();
        if (pool != null && location.equals(pool.getId())) {
          Object o = pool.get(name);
          if (o != null) {
            return o;
          } else {
            continue;
          }
        }

        String poolId = intpGroup.getResourcePoolId();
        if (poolId != null && !poolId.equals(location)) {
          continue;
        }
        interpreterGroupToCheck.add(intpGroup);
      }
    }

    for (InterpreterGroup intpGroup : interpreterGroupToCheck) {
      // remote interpreter's pool
      if (intpGroup.size() == 0) {
        continue;
      }
      Interpreter anyInterpreter = intpGroup.get(0); // because of all remote interpreter
                                                     // in the same group uses
                                                     // the same resource pool.
      if (anyInterpreter == null) {
        continue;
      }

      while (anyInterpreter instanceof WrappedInterpreter){
        anyInterpreter = ((WrappedInterpreter) anyInterpreter).getInnerInterpreter();
      }

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
          ByteBuffer buffer = c.resourcePoolGet(name);
          if (buffer == null) {
            continue;
          } else {
            return deserializeResource(buffer);
          }
        }
      } catch (TException e) {
        e.printStackTrace();
      } finally {
        cf.releaseClient(c);
      }
    }

    return null;
  }

  public static ByteBuffer serializeResource(Object o) throws IOException {
    if (o == null || !(o instanceof Serializable)) {
      return null;
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos;
      oos = new ObjectOutputStream(out);
      oos.writeObject(o);
      oos.close();
      out.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  public static Object deserializeResource(ByteBuffer buf)
      throws IOException, ClassNotFoundException {
    if (buf == null) {
      return null;
    }
    InputStream ins = ByteBufferInputStream.get(buf);
    ObjectInputStream oin;
    Object object = null;

    oin = new ObjectInputStream(ins);
    object = oin.readObject();
    oin.close();
    ins.close();

    return object;
  }


}
