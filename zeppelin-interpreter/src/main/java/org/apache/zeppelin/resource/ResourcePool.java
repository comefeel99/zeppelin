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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
    localPool = new HashMap<ResourceInfo, Object>();
    this.resourcePoolEventHandler = resourcePoolEventHandler;
  }

  public String getId() {
    return id;
  }

  private String generatePoolId() {
    return "resourcePool_" + hashCode();
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
      return resourcePoolEventHandler.resourcePoolSearch(resourcePoolId, namePattern);
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
}
