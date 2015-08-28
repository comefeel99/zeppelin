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

import java.io.Serializable;

/**
 * Information for an object in the ResourcePool
 */
public class ResourceInfo {
  final String location;    // where this resource lives. Usually interpreter id.
  final String name;        // name of resource
  final boolean serializable;

  public ResourceInfo(String location, String name, Object o) {
    this.name = name;
    this.location = location;
    serializable = o instanceof Serializable;
  }

  public boolean equals(Object o) {
    if (o instanceof ResourceInfo) {
      ResourceInfo r = (ResourceInfo) o;
      return r.location.equals(location) && r.name.equals(name);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return ("location:" + location + " name:" + name).hashCode();
  }

  public String location() {
    return location;
  }

  public String name() {
    return name;
  }

  public boolean isSerializable() {
    return serializable;
  }

}
