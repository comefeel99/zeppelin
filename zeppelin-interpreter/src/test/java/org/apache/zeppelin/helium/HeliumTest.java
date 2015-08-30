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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.zeppelin.dep.DependencyResolver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HeliumTest {
  private File tmpDir;
  private File specDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis());
    tmpDir.mkdirs();
    specDir = new File(tmpDir, "specs");
    specDir.mkdir();
  }

  @After
  public void tearDown() throws Exception {
    TestUtils.delete(tmpDir);
  }

  @Test
  public void testHelium() throws IOException {
    HeliumConf conf = HeliumConf.create(new File(tmpDir, "helium.json"));
    ApplicationLoader apploader = new ApplicationLoader(this.getClass().getClassLoader(), new DependencyResolver(tmpDir.getAbsolutePath()));
    Helium helium = new Helium(conf, specDir.getAbsolutePath(), apploader);
    assertEquals(0, helium.getAllSpecs().size());

    // create one spec
    TestUtils.createSpecFile(new File(specDir, "spec1.json"), "artifact1", "class1", "name1", "desc1");
    assertEquals(1, helium.getAllSpecs().size());
    ApplicationSpec spec = helium.getAllSpecs().iterator().next();
    assertFalse(spec.isEnabled());

    // enable the spec
    conf.enable(spec);
    assertTrue(spec.isEnabled());
  }
}
