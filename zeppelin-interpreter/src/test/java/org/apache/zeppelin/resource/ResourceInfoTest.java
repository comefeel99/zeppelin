/* Licensed to the Apache Software Foundation (ASF) under one or more
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

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ResourceInfoTest {

  @Test
  public void testResourceInfo() {
    ResourceInfo r1 = new ResourceInfo("location", "name", "serializable");
    ResourceInfo r2 = new ResourceInfo("location", "name", "another serializable");
    ResourceInfo r3 = new ResourceInfo("location", "name1", "another serializable");

    assertTrue(r1.equals(r2));
    assertFalse(r1.equals(r3));

    Map<ResourceInfo, Object> map = new HashMap<ResourceInfo, Object>();
    map.put(r1, "r1");

    assertEquals("r1", map.get(r1));
    assertEquals("r1", map.get(r2));
  }

}
