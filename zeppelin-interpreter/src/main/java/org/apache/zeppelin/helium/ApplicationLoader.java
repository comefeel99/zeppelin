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

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.interpreter.InterpreterContext;

/**
 * Download necessary jars from maven repository and create application instance
 */
public class ApplicationLoader {
  private final ClassLoader parentClassLoader;
  private final DependencyResolver resolver;
  private final Map<ApplicationSpec, Class<Application>> cached;

  public ApplicationLoader(ClassLoader parentClassLoader, DependencyResolver resolver) {
    this.parentClassLoader = parentClassLoader;
    this.resolver = resolver;
    cached = Collections.synchronizedMap(
        new HashMap<ApplicationSpec, Class<Application>>());
  }

  public Class<Application> load(ApplicationSpec spec) throws Exception {
    if (cached.containsKey(spec)) {
      return cached.get(spec);
    }

    // Create Application classloader
    URL [] urls = new URL[0];

    // load artifact
    if (spec.getMavenArtifact() != null) {
      List<String> paths = resolver.load(spec.getMavenArtifact());

      if (paths != null) {
        List<URL> urlList = new LinkedList<URL>();
        for (String path : paths) {
          urlList.add(new File(path).toURI().toURL());
        }
      }
    }

    URLClassLoader applicationClassLoader = new URLClassLoader(urls, parentClassLoader);

    Class<Application> cls = (Class<Application>)
        Class.forName(spec.getClassName(), true, applicationClassLoader);

    cached.put(spec, cls);
    return cls;
  }

  public void run(ApplicationSpec spec, InterpreterContext context)
      throws Exception {
    run(load(spec), context);
  }

  public void run(Class<Application> appClass, InterpreterContext context)
      throws Exception {
    ClassLoader oldcl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(appClass.getClassLoader());

    try {
      Constructor<Application> constructor =
          appClass.getConstructor(InterpreterContext.class);

      Application app = constructor.newInstance(context);
      app.execute();
    } catch (NoSuchMethodException | SecurityException e) {
      throw e;
    } finally {
      Thread.currentThread().setContextClassLoader(oldcl);
    }
  }
}
