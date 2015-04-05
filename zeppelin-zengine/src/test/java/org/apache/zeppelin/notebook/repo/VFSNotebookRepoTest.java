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

package org.apache.zeppelin.notebook.repo;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.notebook.Note;

public class VFSNotebookRepoTest {
  private File tmpDir;
  private File notebookDir;
  private ZeppelinConfiguration conf;


  @Before
  public void setUp() throws Exception {
    tmpDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis());
    tmpDir.mkdirs();
    new File(tmpDir, "conf").mkdirs();
    notebookDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis()+"/notebook");
    notebookDir.mkdirs();

    System.setProperty(ConfVars.ZEPPELIN_HOME.getVarName(), tmpDir.getAbsolutePath());
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(), notebookDir.getAbsolutePath());

    conf = new ZeppelinConfiguration();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(tmpDir);
  }


  @Test
  public void testCRUD() throws IOException {
    VFSNotebookRepo vfsRepo = new VFSNotebookRepo(conf, notebookDir.toURI());

    // list empty repo
    assertEquals(0, vfsRepo.list().size());

    // create a note
    Note note = new Note(vfsRepo, null, null);
    note.setName("noteA");
    vfsRepo.save(note);

    // list
    assertEquals(1, vfsRepo.list().size());

    // read
    Note noteRead = vfsRepo.get(note.id());
    assertEquals(note.id(), noteRead.id());
    assertEquals(note.getName(), noteRead.getName());

    // remove
    vfsRepo.remove(note.id());
    assertEquals(0, vfsRepo.list().size());

  }

}
