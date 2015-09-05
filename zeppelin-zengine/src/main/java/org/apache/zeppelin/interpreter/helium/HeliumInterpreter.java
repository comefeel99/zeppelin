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
package org.apache.zeppelin.interpreter.helium;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.zeppelin.helium.Helium;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;

/**
 *
 */
public class HeliumInterpreter extends Interpreter {
  static {
    // it's not registering using Interpreter.register()
    // this is special interpreter that available in all notebooks
  }

  public static boolean isInterpreterName(String intpName) {
    // act like this interpreter group is "helium" and interpreter name is "run"
    return "helium.run".equals(intpName) || "helium".equals(intpName) || "run".equals(intpName);
  }

  public HeliumInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {

    HeliumLauncher launcher = new HeliumLauncher(context);

    try {
      launcher.load();
      launcher.run(null);
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    return new InterpreterResult(Code.SUCCESS, "");
  }

  private void getAvailableApps() {

  }


  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.NONE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        HeliumInterpreter.class.getName() + this.hashCode(), 5);
  }

}
