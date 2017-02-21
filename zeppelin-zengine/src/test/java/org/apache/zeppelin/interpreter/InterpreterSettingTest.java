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

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.MockUtil;

import java.util.LinkedList;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * InterpreterSettingTest
 */
public class InterpreterSettingTest implements InterpreterGroupFactory {
  private InterpreterSetting intpSetting;
  MockUtil util = new MockUtil();

  @Before
  public void setUp() {
    intpSetting = new InterpreterSetting();
    intpSetting.setInterpreterGroupFactory(this);
  }

  @Test
  public void testSessionkeyForDefault() {
    // default shared mode
    assertEquals(
        intpSetting.getInterpreterSessionKey("user", "note"),
        intpSetting.getInterpreterSessionKey("user2", "note1"));
  }

  @Test
  public void testSessionkeyForExistingProcess() {
    // when connecting to existing process, only shared session is supported
    intpSetting.getOption().setExistingProcess(true);

    // then any key should be the same
    assertEquals(
        intpSetting.getInterpreterSessionKey("user", "note"),
        intpSetting.getInterpreterSessionKey("user2", "note1"));
  }

  @Test
  public void testSessionkeyForScopedMode() {
    // when scoped mode per note
    intpSetting.getOption().setPerNote(InterpreterOption.SCOPED);
    intpSetting.getOption().setPerUser(InterpreterOption.SHARED);

    // then key should distinguish noteid
    assertEquals(
        intpSetting.getInterpreterSessionKey("user1", "note"),
        intpSetting.getInterpreterSessionKey("user2", "note"));
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user", "note1"),
        intpSetting.getInterpreterSessionKey("user", "note2"));


    // when scoped mode per user
    intpSetting.getOption().setPerNote(InterpreterOption.SHARED);
    intpSetting.getOption().setPerUser(InterpreterOption.SCOPED);

    // then key should distinguish user
    assertEquals(
        intpSetting.getInterpreterSessionKey("user", "note1"),
        intpSetting.getInterpreterSessionKey("user", "note2"));
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user1", "note"),
        intpSetting.getInterpreterSessionKey("user2", "note"));


    // when scoped mode per user
    intpSetting.getOption().setPerNote(InterpreterOption.SCOPED);
    intpSetting.getOption().setPerUser(InterpreterOption.SCOPED);

    // then key should distinguish user and note
    assertEquals(
        intpSetting.getInterpreterSessionKey("user", "note"),
        intpSetting.getInterpreterSessionKey("user", "note"));
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user", "note1"),
        intpSetting.getInterpreterSessionKey("user", "note2"));
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user1", "note"),
        intpSetting.getInterpreterSessionKey("user2", "note"));
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user1", "note1"),
        intpSetting.getInterpreterSessionKey("user2", "note2"));
  }

  @Test
  public void testProcessKeyShouldTheSameToSessionKeyWhenSharedMode() {
    assertNotSame(
        intpSetting.getInterpreterSessionKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user1", "note1"));
  }

  @Test
  public void testProcessKeyShouldTheSameToSessionKeyWhenExistingProcessMode() {
    // when
    intpSetting.getOption().setExistingProcess(true);

    // then
    assertEquals(
        intpSetting.getInterpreterSessionKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user1", "note1"));
  }

  @Test
  public void testProcessKeyPerNoteIsolated() {
    // when per note isolated
    intpSetting.getOption().setPerNote(InterpreterOption.ISOLATED);

    // then
    assertEquals(
        intpSetting.getInterpreterProcessKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user2", "note1"));
    assertNotSame(
        intpSetting.getInterpreterProcessKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user1", "note2"));
  }


  @Test
  public void testProcessKeyPerUserIsolated() {
    // when per user isolated
    intpSetting.getOption().setPerUser(InterpreterOption.ISOLATED);

    // then
    assertEquals(
        intpSetting.getInterpreterProcessKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user1", "note2"));
    assertNotSame(
        intpSetting.getInterpreterProcessKey("user1", "note1"),
        intpSetting.getInterpreterProcessKey("user2", "note1"));
  }

  @Test
  public void getInterpreterGroup() {
    // shared mode returns the same instance
    assertEquals(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user2", "note2"));
  }

  @Test
  public void getInterpreterGroupInScopedMode() {
    // when
    intpSetting.getOption().setPerUser(InterpreterOption.SCOPED);
    intpSetting.getOption().setPerNote(InterpreterOption.SCOPED);

    // then
    assertEquals(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user2", "note2"));
  }


  @Test
  public void getInterpreterGroupInIsolatedMode() {
    // when
    intpSetting.getOption().setPerUser(InterpreterOption.ISOLATED);
    intpSetting.getOption().setPerNote(InterpreterOption.SHARED);

    // then
    assertNotSame(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user2", "note1"));

    // then
    assertEquals(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user1", "note2"));

    // when
    intpSetting.getOption().setPerUser(InterpreterOption.SHARED);
    intpSetting.getOption().setPerNote(InterpreterOption.ISOLATED);

    // then
    assertNotSame(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user1", "note2"));

    // then
    assertEquals(
        intpSetting.getInterpreterGroup("user1", "note1"),
        intpSetting.getInterpreterGroup("user2", "note1"));
  }

  @Test
  public void testCloseAndRemoveInterpreterGroupByUser() {
    intpSetting.getOption().setPerUser(InterpreterOption.SCOPED);

    InterpreterGroup group1 = createSession(intpSetting, "user1", "note1");
    InterpreterGroup group2 = createSession(intpSetting, "user2", "note1");

    assertEquals(group1, group2);
    assertEquals(2, group1.size());
    assertEquals(1, intpSetting.interpreterGroupRef.size());

    intpSetting.closeAndRemoveInterpreterGroupByUser("user1");
    assertEquals(1, group1.size());
    assertEquals(1, intpSetting.interpreterGroupRef.size());

    intpSetting.closeAndRemoveInterpreterGroupByUser("user2");
    assertEquals(0, group1.size());
    assertEquals(0, intpSetting.interpreterGroupRef.size());
  }

  @Override
  public InterpreterGroup createInterpreterGroup(String interpreterGroupId, InterpreterOption option) {
    return new InterpreterGroup();
  }

  private InterpreterGroup createSession(InterpreterSetting setting, String user, String note) {
    InterpreterGroup group = setting.getInterpreterGroup(user, note);
    String session = setting.getInterpreterSessionKey(user, note);
    group.put(session, new LinkedList<Interpreter>());
    return group;
  }
}
