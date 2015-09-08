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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run this server for development mode.
 */
public class ZeppelinApplicationDevServer {
  Logger logger = LoggerFactory.getLogger(ZeppelinApplicationDevServer.class);
  public ZeppelinDevServer server;
  private ApplicationArgument arg;

  public ZeppelinApplicationDevServer(final String className) throws Exception {
    this(ZeppelinDevServer.DEFAULT_TEST_INTERPRETER_PORT, className);
  }

  public ZeppelinApplicationDevServer(int port, final String className)
      throws Exception {
    String localDepRepoDir = "/tmp/local-repo";

    server = new ZeppelinDevServer(port, localDepRepoDir,
        new DevInterpreter.InterpreterEvent() {

        Application app = null;

        @Override
        public InterpreterResult interpret(String st, InterpreterContext context) {
          if (app == null) {
            try {
              app = (Application) ClassLoader.getSystemClassLoader().loadClass(className)
                .newInstance();
              logger.info("Load application " + app.getClass().getName());
              app.load();
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | SecurityException | ClassNotFoundException | IOException e) {
              throw new InterpreterException(e);
            }
          }
          try {
            logger.info("Run application " + app.getClass().getName());
            context.out.setHeader("%angular ");
            app.run(arg, context);
          } catch (IOException e) {
            throw new InterpreterException(e);
          }
          return new InterpreterResult(Code.SUCCESS, "");
        }
      });
  }

  public void setArgument(ApplicationArgument arg) {
    this.arg = arg;
  }

}
