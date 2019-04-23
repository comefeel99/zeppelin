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
package org.apache.zeppelin.webstore;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

public class DocOpTest {
  @Before
  public void setUp() {
  }

  @Test
  public void testCreateObjectInRoot() {
    // add color: yellow to object {name: apple}
    DocOp op = DocOp.set("$.color", "yellow");
    Map doc = (Map) op.apply(ImmutableMap.of("name", "apple"), Map.class);
    assertEquals("apple", doc.get("name"));
    assertEquals("yellow", doc.get("color"));

    // update to color: green
    doc = (Map) DocOp.set("$.color", "green").apply(doc, Map.class);
    assertEquals("apple", doc.get("name"));
    assertEquals("green", doc.get("color"));
  }


  @Test
  public void testUpdateObjectInRoot() {
    DocOp op = DocOp.set("$.name", "orange");
    Map doc = (Map) op.apply(ImmutableMap.of("name", "apple"), Map.class);
    assertEquals("orange", doc.get("name"));
  }

  @Test
  public void testCreateObjectInChildPathAndUpdate() {
    DocOp op = DocOp.set("$.people.name", "orange");
    Map doc = (Map) op.apply(ImmutableMap.of("people", ImmutableMap.of()), Map.class);
    assertEquals("orange", ((Map) doc.get("people")).get("name"));
  }

  @Test
  public void testDeleteObject() {
    DocOp op = DocOp.delete("$.people.temp");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "people", ImmutableMap.of("age", 30.0, "temp", true),
            "color", "green"), Map.class);
    assertEquals(30.0, ((Map) doc.get("people")).get("age"));
    assertEquals(null, ((Map) doc.get("people")).get("temp"));
    assertEquals("green", doc.get("color"));
  }

  @Test
  public void testDeleteNotExistingPath() {
    DocOp op = DocOp.delete("$.people.temp");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "color", "green"), Map.class);
    assertEquals(null, doc);
  }

  @Test
  public void testGetObject() {
    DocOp op = DocOp.get("$.people");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "people", ImmutableMap.of("age", 30.0, "temp", true),
            "color", "green"), Map.class);

    assertEquals(30.0, doc.get("age"));
    assertEquals(true, doc.get("temp"));
  }

  @Test
  public void testGetObjectNotExistingPath() {
    DocOp op = DocOp.get("$.notexists");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "color", "green"), Map.class);

    assertEquals(null, doc);
  }

  @Test
  public void testMoveNotexistsPath() {
    DocOp op = DocOp.move("$.notexists", "$.target");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "color", "green"), Map.class);

    assertEquals(ImmutableMap.of("color", "green"), doc);
  }

  @Test
  public void testMove() {
    DocOp op = DocOp.move("$.color", "$.sound");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "color", "green"), Map.class);

    assertEquals(ImmutableMap.of("sound", "green"), doc);
  }

  @Test
  public void testMoveOverride() {
    DocOp op = DocOp.move("$.color", "$.sound");
    Map doc = (Map) op.apply(ImmutableMap.of(
            "color", "green",
            "sound", "large"), Map.class);

    assertEquals(ImmutableMap.of("sound", "green"), doc);
  }

  @Test
  public void testType() {
    Type type = new ParameterizedType() {

      @Override
      public Type[] getActualTypeArguments() {
        return new Type[]{
                java.util.Date.class
        };
      }

      @Override
      public Type getRawType() {
        return java.util.List.class;
      }

      @Override
      public Type getOwnerType() {
        return null;
      }
    };
    Class c = (Class) type.getClass();
    Gson gson = new Gson();
    LinkedList list = new LinkedList();
    list.add(new Date());

    List<Date> des = gson.fromJson(gson.toJson(list), type);
    System.out.println(des.get(0).getTime());
    System.out.println(new Date().getClass().getTypeName());
  }
}