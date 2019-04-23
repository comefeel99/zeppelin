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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

/**
 * Manipulate Json document.
 * It can add, update, delete nested json object.
 */
public class DocOp {
  public static final String ROOT_PATH = "$";
  private static transient Gson gson = new Gson();

  /**
   * Type of manipulating Json object operation.
   */
  public enum Type {
    GET,
    SET,          // set value of given path
    DELETE,
    SET_DATE,     // set current date to given path
    MOVE,
  };

  private Type type;
  private String path;
  private Object value;

  DocOp(Type type, String path, String key, Object value) {
    this.type = type;
    this.path = path;
    this.value = value;
  }

  // simple constructor for Jackson
  DocOp() {
  }

  public static DocOp set(String jsonPath, Object value) {
    return new DocOp(Type.SET, jsonPath, null, value);
  }

  public static DocOp setDate(String jsonPath) {
    return new DocOp(Type.SET_DATE, jsonPath, null, null);
  }

  public static DocOp delete(String jsonPath) {
    return new DocOp(Type.DELETE, jsonPath, null, null);
  }

  public static DocOp get(String jsonPath) {
    return new DocOp(Type.GET, jsonPath, null, null);
  }

  public static DocOp move(String fromJsonPath, String toJsonPath) {
    return new DocOp(Type.MOVE, fromJsonPath, null, toJsonPath);
  }

  public Object apply(Map doc, Class returnType) {
    JsonPath jsonPath = JsonPath.compile(path);
    validatePath(path, jsonPath);

    switch (type) {
      case GET:
        try {
          return gson.fromJson(jsonPath.read(gson.toJson(doc)).toString(), returnType);
        } catch (PathNotFoundException e) {
          return null;
        }
      case SET:
        // if path does not exists, update
        doc = createPathIfDoesNotExists(path, doc);
        String jsonString = gson.toJson(doc);
        return gson.fromJson(JsonPath.parse(jsonString).set(jsonPath, value).jsonString(), returnType);
      case SET_DATE:
        doc = createPathIfDoesNotExists(path, doc);
        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern(Const.DATE_FORMAT));
        return gson.fromJson(JsonPath.parse(gson.toJson(doc))
                .set(jsonPath, now).jsonString(), returnType);
      case DELETE:
        try {
          return gson.fromJson(JsonPath.parse(gson.toJson(doc)).delete(jsonPath).jsonString(), returnType);
        } catch (PathNotFoundException e) {
          return null;
        }
      case MOVE:
        try {
          String targetJsonPath = (String) value;
          Object movingObject = gson.fromJson(jsonPath.read(gson.toJson(doc)).toString(), Object.class);
          Object docRemoved = gson.fromJson(JsonPath.parse(gson.toJson(doc)).delete(jsonPath).jsonString(), returnType);
          Object moving = createPathIfDoesNotExists(targetJsonPath, (Map) docRemoved);
          return gson.fromJson(JsonPath.parse(gson.toJson(moving)).set(targetJsonPath, movingObject).jsonString(),
                  returnType);
        } catch (PathNotFoundException e) {
          // nothing to move
          return doc;
        }
      default:
        return doc;
    }
  }

  public static Map deepCopy(Map doc) {
    return gson.fromJson(gson.toJson(doc), Map.class);
  }

  private Map createPathIfDoesNotExists(String path, Map doc) {
    JsonPath jPath = JsonPath.compile(path);
    try {
      jPath.read(gson.toJson(doc));
      return doc;
    } catch (PathNotFoundException e) {
      int pos = path.lastIndexOf('.');
      if (pos <= 0) {
        throw e;
      } else {
        String parentPath = path.substring(0, pos);
        String key = path.substring(pos + 1);
        doc = createPathIfDoesNotExists(parentPath, doc);
        return gson.fromJson(JsonPath.parse(gson.toJson(doc))
                .put(JsonPath.compile(parentPath), key, new HashMap()).jsonString(), Map.class);
      }
    }
  }

  public Type getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public Object getValue() {
    return value;
  }

  private void validatePath(String path, JsonPath jsonPath) throws InvalidPathException {
    if (!jsonPath.isDefinite()) {
      throw new InvalidPathException("JsonPath " + jsonPath + " is not definite");
    }
    if (path == null || path.isEmpty()) {
      throw new InvalidPathException("Empty path");
    }

    if (path.contains("..")) {
      throw new InvalidPathException("Path includes empty path name");
    }

    if (path.contains("*")) {
      throw new InvalidPathException("Wildcard in the path not supported yet");
    }

    if (path.contains("[") || path.contains("]")) {
      throw new InvalidPathException("List index in the path not supported yet");
    }
  }

  static {
    Configuration.setDefaults(new Configuration.Defaults() {

      private final JsonProvider jsonProvider = new GsonJsonProvider();
      private final MappingProvider mappingProvider = new GsonMappingProvider();

      @Override
      public JsonProvider jsonProvider() {
        return jsonProvider;
      }

      @Override
      public MappingProvider mappingProvider() {
        return mappingProvider;
      }

      @Override
      public Set<Option> options() {
        return EnumSet.noneOf(Option.class);
      }
    });
  }
}
