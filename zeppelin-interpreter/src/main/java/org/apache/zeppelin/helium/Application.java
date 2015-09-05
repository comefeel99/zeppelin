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

import java.io.IOException;
import java.util.List;

import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.InterpreterContext;

/**
 * Base class for ZeppelinApplication
 */
public abstract class Application {
  private InterpreterContext context;
  private Watcher watcher;

  private class Watcher extends AngularObjectWatcher {

    private String name;

    public Watcher(String name, InterpreterContext context) {
      super(context);
      this.name = name;
    }

    @Override
    public void watch(Object oldObject, Object newObject, InterpreterContext context) {
      onChange(name, oldObject, newObject);
    }
  }

  public Application(InterpreterContext context) {
    this.context = context;
  }

  /**
   * On change watching data
   * @param name
   * @param oldObject
   * @param newObject
   */
  protected abstract void onChange(String name, Object oldObject, Object newObject);

  /**
   * Send signal to this application.
   */
  public abstract void signal(Signal signal);

  /**
   * Load this application
   * @return
   * @throws IOException
   */
  public abstract void load() throws ApplicationException, IOException;

  /**
   * Load this application
   * @return
   * @throws IOException
   */
  public abstract void run(ApplicationArgument arg) throws ApplicationException, IOException;


  /**
   * Unload this application
   * @return
   * @throws IOException
   */
  public abstract void unload() throws ApplicationException, IOException;



  /**
   * Get interpreter context that this application is running
   * @return
   */
  public InterpreterContext getInterpreterContext() {
    return context;
  }

  protected Object getResourceFromPool(String resourceId) {
    return context.getResourcePool().get(resourceId);
  }

  /**
   * Bind object to angular scope
   * @param name
   * @param o
   */
  protected void bind(String name, Object o) {
    AngularObjectRegistry registry = context.getAngularObjectRegistry();
    registry.add(name, o, context.getNoteId(), context.getParagraphId());
  }

  protected void unbind(String name, Object o) {
    AngularObjectRegistry registry = context.getAngularObjectRegistry();
    registry.remove(name, context.getNoteId(), context.getParagraphId());
  }

  protected void watch(String name) {
    AngularObjectRegistry registry = context.getAngularObjectRegistry();
    AngularObject ao = registry.get(name, context.getNoteId(), context.getParagraphId());
    if (ao != null) {
      ao.addWatcher(new Watcher(name, context));
    }
  }

  protected void unwatch(String name) {
    AngularObjectRegistry registry = context.getAngularObjectRegistry();
    AngularObject ao = registry.get(name, context.getNoteId(), context.getParagraphId());
    if (ao != null) {
      ao.clearAllWatchers();
    }
  }
}
