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
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.apache.zeppelin.interpreter.remote.mock.MockInterpreterA;
import org.apache.zeppelin.interpreter.remote.mock.MockInterpreterB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ResourcePoolTest {
  private RemoteInterpreterServer server;

  @Before
  public void setUp() throws InterruptedException, TTransportException, IOException {
    server = new RemoteInterpreterServer(
        RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces());
    assertEquals(false, server.isRunning());

    server.start();
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        break;
      } else {
        Thread.sleep(200);
      }
    }
  }

  @After
  public void tearDown() throws TException, InterruptedException {
    long startTime = System.currentTimeMillis();
    server.shutdown();

    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        Thread.sleep(200);
      } else {
        break;
      }
    }
  }


  @Test
  public void testLocalAccess() {
    ResourcePool pool = new ResourcePool(server);

    assertNull(pool.get("name1"));
    pool.put("name1", "someobject");

    assertEquals("someobject", pool.get("name1"));

    assertEquals(0, pool.search("name").size());
    assertEquals(1, pool.search("name1").size());
    assertEquals(1, pool.search("na.*[0-9]").size());
  }


}
