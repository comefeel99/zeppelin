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
package org.apache.zeppelin.webrpc;

import com.google.gson.Gson;
import org.apache.zeppelin.webrpc.security.AclContext;
import org.apache.zeppelin.webrpc.security.WebRpcAcl;
import org.apache.zeppelin.webrpc.security.WebRpcPermissionDeniedException;
import org.apache.zeppelin.webrpc.transport.Session;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class RpcRef {
  private final WebRpcServer rpcServer;
  private final WebRpcAcl webRpcAcl;
  private final AclContext aclContext;
  private final Session session;
  private final String rpcName;
  private final Gson gson = new Gson();

  public RpcRef(WebRpcServer rpcServer, WebRpcAcl webRpcAcl, AclContext aclContext, Session session, String rpcName) {
    this.rpcServer = rpcServer;
    this.webRpcAcl = webRpcAcl;
    this.aclContext = aclContext;
    this.session = session;
    this.rpcName = rpcName;
  }

  /**
   * WebRpcInvokeMessage and return result, with type inference.
   */
  public Object invoke(String methodName, Object [] params)
          throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException, WebRpcPermissionDeniedException {
    return invoke(methodName, (Type[]) null, params);
  }

  /**
   * WebRpcInvokeMessage and return result.
   */
  public Object invoke(String methodName, String [] typeNames, Object [] params)
          throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException, WebRpcPermissionDeniedException {
    Type [] types = typeFromName(typeNames);
    if (params == null) {
      params = new Object[] {};
    }
    return invoke(methodName, types, params);
  }

  /**
   * WebRpcInvokeMessage and return result.
   */
  public Object invoke(String methodName, Type [] types, Object [] params)
          throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          WebRpcPermissionDeniedException, ClassNotFoundException {
    Object rpcObject = rpcServer.getRpcObject(session.getId(), rpcName);

    boolean permanentObject = false;
    if (rpcObject == null) {
      rpcObject = rpcServer.getRpcObject(rpcName);
      permanentObject = true;
    }

    if (rpcObject == null) {
      throw new WebRpcPermissionDeniedException("No rpc object " + rpcName + " available");
    }

    Type[] methodTypes = null;
    Object [] methodParams = null;
    if (types != null) {
      methodTypes = types;
      methodParams = params;
    } else {
      // inference method param types
      boolean found = false;
      Method[] methods = rpcObject.getClass().getDeclaredMethods();
      for (Method m : methods) {
        if (!m.getName().equals(methodName)) {
          continue;
        }
        Type[] paramTypes = m.getGenericParameterTypes();
        Object[] paramValues = new Object[paramTypes.length];

        int pidx = 0;
        for (int i = 0; i < paramTypes.length; i++) {
          Type t = paramTypes[i];
          if (t.getTypeName().equals(AclContext.class.getName()) ||
                  t.getTypeName().equals(Session.class.getName())) {
            paramValues[i] = null;
          } else {
            if (pidx == params.length) {  // not enough param for this method signature
              continue;
            } else {
              paramValues[i] = params[pidx++];
            }
          }
        }

        if (pidx == params.length) {  // param number does not match
          found = true;
          methodParams = paramValues;
          methodTypes = paramTypes;
          break;
        }
      }

      if (!found) {
        throw new WebRpcPermissionDeniedException("No method found for given parameters");
      }
    }
    Class[] classes = classFromType(methodTypes);

    webRpcAcl.check(aclContext, rpcName, methodName, classes, methodParams, rpcObject, permanentObject);

    Method method = rpcObject.getClass().getMethod(
            methodName,
            classes);
    method.setAccessible(true);
    return method.invoke(rpcObject, convertParams(methodTypes, methodParams));
  }

  private ParameterizedType [] typeFromName(String [] classNames) throws ClassNotFoundException {
    if (classNames == null) {
      return null;
    }
    ParameterizedType[] types = new ParameterizedType[classNames.length];
    for (int i = 0; i < classNames.length; i++) {
      types[i] = typeFromName(classNames[i]);
    }
    return types;
  }

  private ParameterizedType typeFromName(String commaSeparatedClasses) throws ClassNotFoundException {
    String[] classNames = commaSeparatedClasses.split(",");
    Class [] arguments;

    if (classNames.length > 1) {
      arguments = new Class[classNames.length - 1];
      for (int i = 1; i < classNames.length; i++) {
        arguments[i - 1] = loadClass(classNames[i]);
      }
    } else {
      arguments = new Class[0];
    }

    Class rawType = loadClass(classNames[0]);

    return new ParameterizedType() {
      @Override
      public Type[] getActualTypeArguments() {
        return arguments;
      }

      @Override
      public Type getRawType() {
        return rawType;
      }

      @Override
      public Type getOwnerType() {
        return null;
      }
    };
  }

  private Class [] classFromType(Type [] types) throws ClassNotFoundException {
    Class[] cls = new Class[types.length];
    for (int i = 0; i < types.length; i++) {
      if (types[i] instanceof ParameterizedType) {
        String typeName = ((ParameterizedType) types[i]).getRawType().getTypeName();
        cls[i] = loadClass(typeName);
      } else {
        cls[i] = loadClass(types[i].getTypeName());
      }
    }
    return cls;
  }

  private Object [] convertParams(Type[] types, Object [] params) {
    Object [] converted = new Object[types.length];

    for (int i = 0; i < types.length; i++) {
      Type type = types[i];
      String typeName;
      if (type instanceof ParameterizedType) {
        typeName = ((ParameterizedType) type).getRawType().getTypeName();
      } else {
        typeName = type.getTypeName();
      }

      Object param = params[i];
      if (typeName.equals(AclContext.class.getName())) {
        converted[i] = aclContext;
      } else if (typeName.equals(Session.class.getName())) {
        converted[i] = session;
      } else if (param == null) {
        converted[i] = null;
      } else if (param.getClass().getName().equals(typeName)) {
        converted[i] = param;
      } else {
        // try to convert param
        converted[i] = gson.fromJson(gson.toJson(param), type);
      }
    }

    return converted;
  }

  private Class loadClass(String className) throws ClassNotFoundException {
    switch(className) {
      case "byte":
        return byte.class;
      case "short":
        return short.class;
      case "int":
        return int.class;
      case "long":
        return long.class;
      case "float":
        return float.class;
      case "double":
        return double.class;
      case "boolean":
        return boolean.class;
      case "char":
        return char.class;
      default:
        return getClass().getClassLoader().loadClass(className);
    }
  }
}
