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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.remote.mock.MockInterpreterResourcePool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ResourcePoolTest implements ResourcePoolEventHandler {
  private InterpreterGroup intpGroup1;
  private InterpreterGroup intpGroup2;
  private HashMap<String, String> env;
  String localRepo = System.getProperty("java.io.tmpdir") + "/localrepo";

  @Before
  public void setUp() throws Exception {
    intpGroup1 = new InterpreterGroup();
    intpGroup2 = new InterpreterGroup();
    env = new HashMap<String, String>();
    env.put("ZEPPELIN_CLASSPATH", new File("./target/test-classes").getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    intpGroup1.clone();
    intpGroup1.destroy();
    intpGroup2.clone();
    intpGroup2.destroy();

  }

  @Test
  public void testLocalAccess() {
    ResourcePool pool = new ResourcePool(this);

    assertNull(pool.get("name1"));
    pool.put("name1", "someobject");

    assertEquals("someobject", pool.get("name1"));

    assertEquals(0, pool.search("name").size());
    assertEquals(1, pool.search("name1").size());
    assertEquals(1, pool.search("na.*[0-9]").size());
  }

  @Test
  public void testRemotePool() {
    Gson gson = new Gson();
    Properties p = new Properties();

    RemoteInterpreter intpA = new RemoteInterpreter(
        p,
        MockInterpreterResourcePool.class.getName(),
        new File("../bin/interpreter.sh").getAbsolutePath(),
        "fake",
        env,
        10 * 1000,
        localRepo
        );

    intpGroup1.add(intpA);
    intpA.setInterpreterGroup(intpGroup1);

    RemoteInterpreter intpB = new RemoteInterpreter(
        p,
        MockInterpreterResourcePool.class.getName(),
        new File("../bin/interpreter.sh").getAbsolutePath(),
        "fake",
        env,
        10 * 1000,
        localRepo
        );

    intpGroup2.add(intpB);
    intpB.setInterpreterGroup(intpGroup2);

    intpA.open();
    intpB.open();

    // empty items in resource pool
    Collection<ResourceInfo> infos = getResourceInfoFromResult(intpA.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext()).message());
    assertEquals(0, infos.size());

    InterpreterResult ret = intpA.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext());
    assertEquals("[]", ret.message());

    // add one resource
    intpA.interpret("put r1 o1", createInterpreterContext());

    // search resource from intp A
    infos = getResourceInfoFromResult(intpA.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext()).message());
    assertEquals(1, infos.size());

    // get resource from intp A
    ResourceInfo info = infos.iterator().next();
    assertEquals("o1", gson.fromJson(intpA.interpret("get " + info.location() + " " + info.name(), createInterpreterContext()).message(), String.class));

    // search resource from intp B
    infos = getResourceInfoFromResult(intpB.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext()).message());
    assertEquals(1, infos.size());

    // get resource from intp B
    assertEquals("o1", gson.fromJson(intpB.interpret("get " + info.location() + " " + info.name(), createInterpreterContext()).message(), String.class));

    // remove resource
    intpA.interpret("remove " + info.name(), createInterpreterContext());

    // search resource from intp A
    infos = getResourceInfoFromResult(intpA.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext()).message());
    assertEquals(0, infos.size());

    // search resource from intp B
    infos = getResourceInfoFromResult(intpB.interpret("search " + ResourcePool.LOCATION_ANY + " " + ResourcePool.NAME_ANY, createInterpreterContext()).message());
    assertEquals(0, infos.size());

    intpA.close();
    intpB.close();
  }

  private Collection<ResourceInfo> getResourceInfoFromResult(String message) {
    Gson gson = new Gson();
    return gson.fromJson(message, new TypeToken<Collection<ResourceInfo>>(){}.getType());
  }

  @Override
  public Collection<ResourceInfo> resourcePoolSearch(String location, String namePattern) {
    return null;
  }

  @Override
  public Object resourcePoolGetObject(String location, String name) {
    return null;
  }

  private InterpreterContext createInterpreterContext() {
    return new InterpreterContext(
        "note",
        "id",
        "title",
        "text",
        new HashMap<String, Object>(),
        new GUI(),
        new AngularObjectRegistry(intpGroup1.getId(), null),
        new LinkedList<InterpreterContextRunner>(),
        new InterpreterOutput(),
        new ResourcePool(null));
  }
}
