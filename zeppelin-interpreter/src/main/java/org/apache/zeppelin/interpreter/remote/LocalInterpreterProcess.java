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
package org.apache.zeppelin.interpreter.remote;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * creates interpreter process locally
  */
public class LocalInterpreterProcess implements
    InterpreterProcess, ExecuteResultHandler {
  Logger logger = LoggerFactory.getLogger(LocalInterpreterProcess.class);
  private final int PROCESS_TERMINATION_TIMEOUT_SEC = 30 * 1000;

  private final String interpreterRunner;
  private final String interpreterPath;
  private final int connectTimeout;
  private final Map<String, String> env;

  private int port;
  private DefaultExecutor executor;
  private ExecuteWatchdog watchdog;
  private boolean running;


  public LocalInterpreterProcess (
      String interpreterRunner,
      String interpreterPath,
      Map<String, String> env,
      int connectTimeout) {
    this.interpreterRunner = interpreterRunner;
    this.interpreterPath = interpreterPath;
    this.connectTimeout = connectTimeout;
    this.env = env;
  }

  @Override
  public void start() {
    try {
      this.port = RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces();
    } catch (IOException e1) {
      throw new InterpreterException(e1);
    }


    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    cmdLine.addArgument("-d", false);
    cmdLine.addArgument(interpreterPath, false);
    cmdLine.addArgument("-p", false);
    cmdLine.addArgument(Integer.toString(port), false);

    executor = new DefaultExecutor();

    watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    running = true;
    try {
      Map procEnv = EnvironmentUtils.getProcEnvironment();
      procEnv.putAll(env);

      logger.info("Run interpreter process {}", cmdLine);
      executor.execute(cmdLine, procEnv, this);
    } catch (IOException e) {
      running = false;
      throw new InterpreterException(e);
    }


    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < connectTimeout) {
      if (RemoteInterpreterUtils.checkIfRemoteEndpointAccessible("localhost", port)) {
        break;
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
        }
      }
    }

  }

  @Override
  public void stop() {
    synchronized (this) {
      if (isRunning()) {
        logger.info("kill interpreter process");
        watchdog.destroyProcess();
      }

      if (isRunning()) {
        try {
          this.wait(PROCESS_TERMINATION_TIMEOUT_SEC);
        } catch (InterruptedException e) {
          logger.error("Error", e);
        }
      }
    }
  }

  @Override
  public String getHost() {
    return "localhost";
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public void onProcessComplete(int exitValue) {
    logger.info("Interpreter process exited {}", exitValue);
    synchronized (this) {
      running = false;
      this.notifyAll();
    }
  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    logger.info("Interpreter process failed {}", e);
    synchronized (this) {
      running = false;
      this.notifyAll();
    }
  }

}
