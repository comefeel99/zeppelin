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

import java.util.List;
import java.util.Map;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.helium.ApplicationLoader;
import org.apache.zeppelin.resource.ResourcePool;

/**
 * Interpreter context
 */
public class InterpreterContext {
  private final String noteId;
  private final String paragraphTitle;
  private final String paragraphId;
  private final String paragraphText;
  private final Map<String, Object> config;
  public final InterpreterOutput out;
  private final ResourcePool resourcePool;
  private GUI gui;
  private AngularObjectRegistry angularObjectRegistry;
  private List<InterpreterContextRunner> runners;



  public InterpreterContext(String noteId,
                            String paragraphId,
                            String paragraphTitle,
                            String paragraphText,
                            Map<String, Object> config,
                            GUI gui,
                            AngularObjectRegistry angularObjectRegistry,
                            List<InterpreterContextRunner> runners,
                            InterpreterOutput out,
                            ResourcePool resourcePool
                            ) {
    this.noteId = noteId;
    this.paragraphId = paragraphId;
    this.paragraphTitle = paragraphTitle;
    this.paragraphText = paragraphText;
    this.config = config;
    this.gui = gui;
    this.angularObjectRegistry = angularObjectRegistry;
    this.runners = runners;
    this.out = out;
    this.resourcePool = resourcePool;
  }


  public String getNoteId() {
    return noteId;
  }

  public String getParagraphId() {
    return paragraphId;
  }

  public String getParagraphText() {
    return paragraphText;
  }

  public String getParagraphTitle() {
    return paragraphTitle;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public GUI getGui() {
    return gui;
  }

  public ResourcePool getResourcePool() {
    return resourcePool;
  }

  public AngularObjectRegistry getAngularObjectRegistry() {
    return angularObjectRegistry;
  }

  public List<InterpreterContextRunner> getRunners() {
    return runners;
  }

  // Thread local variable containing each thread's ID
  private static final ThreadLocal<InterpreterContext> threadLocalInterpreterContexts =
      new ThreadLocal<InterpreterContext>() {

      @Override
      protected InterpreterContext initialValue() {
        return null;
      }
    };

  /**
   * Return interpretercontext that can associated with current thread
   * @return
   */
  public static InterpreterContext getCurrentInterpreterContext() {
    return threadLocalInterpreterContexts.get();
  }

  /**
   * Associate interpretercontext with current thread
   */
  public static void setCurrentInterpreterContext(InterpreterContext context) {
    threadLocalInterpreterContexts.set(context);
  }

  /**
   * Associate interpretercontext with current thread
   */
  public static void removeCurrentInterpreterContext() {
    threadLocalInterpreterContexts.remove();
  }

}
