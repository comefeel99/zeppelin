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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.zeppelin.helium.Application;
import org.apache.zeppelin.helium.ApplicationArgument;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;

/**
 * Run this server for development mode.
 */
public class ZeppelinApplicationDevServer {

  public ZeppelinDevServer server;

  public ZeppelinApplicationDevServer(final String className, final ApplicationArgument arg)
      throws Exception {
    int port = ZeppelinDevServer.DEFAULT_TEST_INTERPRETER_PORT;
    String localDepRepoDir = System.getProperty("java.io.tmpdir") + "/localrepo";

    server = new ZeppelinDevServer(port, localDepRepoDir,
        new DevInterpreter.InterpreterEvent() {

        Application app = null;

        @Override
        public InterpreterResult interpret(String st, InterpreterContext context) {
          if (app == null) {
            try {
              app = (Application) ClassLoader.getSystemClassLoader().loadClass(className)
                .getConstructor(InterpreterContext.class).newInstance(context);
              app.load();
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException
                | ClassNotFoundException | IOException e) {
              throw new InterpreterException(e);
            }
          }
          try {
            app.run(arg);
          } catch (IOException e) {
            throw new InterpreterException(e);
          }
          return new InterpreterResult(Code.SUCCESS, "");
        }
      });
  }

}
