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
package org.apache.zeppelin.interpreter.dev;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.ClassloaderInterpreter;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterOutputChangeListener;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.dev.DevInterpreter.InterpreterEvent;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer;

/**
 *
 */
public abstract class ZeppelinApplicationDevServer extends
    RemoteInterpreterServer implements InterpreterEvent, InterpreterOutputChangeListener {
  public static final int DEFAULT_TEST_INTERPRETER_PORT = 29914;

  DevInterpreter interpreter = null;
  InterpreterOutput out = null;

  public ZeppelinApplicationDevServer(int port, String localRepo) throws TException {
    super(port, localRepo);
  }

  @Override
  protected Interpreter getInterpreter(String className) throws TException {
    synchronized (this) {
      if (interpreter == null) {
        Properties p = new Properties();
        interpreter = new DevInterpreter(p, this);
      }
      notify();
    }
    return new ClassloaderInterpreter(interpreter, this.getClass().getClassLoader());
  }

  @Override
  protected InterpreterOutput createInterpreterOutput() {
    if (out == null) {
      try {
        out =  new InterpreterOutput(this);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return out;
  }

  @Override
  public void fileChanged(File file) {
    refresh();
  }

  @Override
  public boolean clearInterpreterOutput() {
    return false;
  }

  /**
   * execute application. This will be called when you run %dev paragraph in Zeppelin
   * @throws Exception
   */
  protected abstract void onInterpret(InterpreterContext context) throws Exception;

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    try {
      onInterpret(context);
    } catch (Exception e) {
      throw new InterpreterException(e);
    }
    return new InterpreterResult(Code.SUCCESS, "");
  }

  public void refresh() {
    interpreter.rerun();
  }

  /**
   * Wait until %dev paragraph is executed and connected to this process
   */
  public void waitForConnected() {
    synchronized (this) {
      while (!isConnected()) {
        try {
          this.wait(10 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public boolean isConnected() {
    return !(interpreter == null || interpreter.getLastInterpretContext() == null);
  }
}
