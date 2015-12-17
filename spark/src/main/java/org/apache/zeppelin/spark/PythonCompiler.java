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
package org.apache.zeppelin.spark;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Python compiler instance
 */
public class PythonCompiler implements ExecuteResultHandler {
  Logger logger = LoggerFactory.getLogger(PythonCompiler.class);

  private DefaultExecutor executor;
  private ByteArrayOutputStream outputStream;
  private PipedInputStream in;
  private BufferedWriter ins;
  private ByteArrayOutputStream input;
  private boolean pythonscriptRunning = false;

  public PythonCompiler() {

  }

  public void init(String compilerId, String pythonPath, String scriptPath, int gatewayPort,
      int sparkVersion) {
    // Run python shell
    CommandLine cmd = CommandLine.parse(pythonPath);
    cmd.addArgument(scriptPath, false);
    cmd.addArgument(Integer.toString(gatewayPort), false);
    cmd.addArgument(Integer.toString(sparkVersion), false);
    cmd.addArgument(compilerId, false);
    executor = new DefaultExecutor();
    outputStream = new ByteArrayOutputStream();
    PipedOutputStream ps = new PipedOutputStream();
    in = null;
    try {
      in = new PipedInputStream(ps);
    } catch (IOException e1) {
      throw new InterpreterException(e1);
    }
    ins = new BufferedWriter(new OutputStreamWriter(ps));

    input = new ByteArrayOutputStream();

    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, outputStream, in);
    executor.setStreamHandler(streamHandler);
    executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));


    try {
      Map env = EnvironmentUtils.getProcEnvironment();

      executor.execute(cmd, env, this);
      pythonscriptRunning = true;
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    outputStream.reset();
    synchronized (pythonScriptInitializeNotifier) {
      long startTime = System.currentTimeMillis();
      while (pythonScriptInitialized == false
          && pythonscriptRunning
          && System.currentTimeMillis() - startTime < 10 * 1000) {
        try {
          pythonScriptInitializeNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    try {
      input.write("import sys, getopt\n".getBytes());
      ins.flush();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
  }


  public InterpreterResult interpret(String st, InterpreterContext context,
      SparkInterpreter sparkInterpreter) {
    if (!pythonscriptRunning) {
      return new InterpreterResult(Code.ERROR, "python process not running"
          + outputStream.toString());
    }

    outputStream.reset();

    synchronized (pythonScriptInitializeNotifier) {
      long startTime = System.currentTimeMillis();
      while (pythonScriptInitialized == false
          && pythonscriptRunning
          && System.currentTimeMillis() - startTime < 10 * 1000) {
        try {
          pythonScriptInitializeNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (pythonscriptRunning == false) {
      // python script failed to initialize and terminated
      return new InterpreterResult(Code.ERROR, "failed to start pyspark"
          + outputStream.toString());
    }
    if (pythonScriptInitialized == false) {
      // timeout. didn't get initialized message
      return new InterpreterResult(Code.ERROR, "pyspark is not responding "
          + outputStream.toString());
    }

    if (!sparkInterpreter.getSparkVersion().isPysparkSupported()) {
      return new InterpreterResult(Code.ERROR, "pyspark "
          + sparkInterpreter.getSparkContext().version() + " is not supported");
    }
    String jobGroup = sparkInterpreter.getJobGroup(context);
    ZeppelinContext z = sparkInterpreter.createOrGetScalaCompiler(context).getZeppelinContext();
    z.setInterpreterContext(context);
    z.setGui(context.getGui());
    pythonInterpretRequest = new PythonInterpretRequest(st, jobGroup);
    statementOutput = null;

    synchronized (statementSetNotifier) {
      statementSetNotifier.notify();
    }

    synchronized (statementFinishedNotifier) {
      while (statementOutput == null) {
        try {
          statementFinishedNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (statementError) {
      return new InterpreterResult(Code.ERROR, statementOutput);
    } else {
      return new InterpreterResult(Code.SUCCESS, statementOutput);
    }

  }




  PythonInterpretRequest pythonInterpretRequest = null;

  /**
   *
   */
  public class PythonInterpretRequest {
    public String statements;
    public String jobGroup;

    public PythonInterpretRequest(String statements, String jobGroup) {
      this.statements = statements;
      this.jobGroup = jobGroup;
    }

    public String statements() {
      return statements;
    }

    public String jobGroup() {
      return jobGroup;
    }
  }

  Integer statementSetNotifier = new Integer(0);

  public PythonInterpretRequest getStatements() {
    synchronized (statementSetNotifier) {
      while (pythonInterpretRequest == null) {
        try {
          statementSetNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
      PythonInterpretRequest req = pythonInterpretRequest;
      pythonInterpretRequest = null;
      return req;
    }
  }

  String statementOutput = null;
  boolean statementError = false;
  Integer statementFinishedNotifier = new Integer(0);

  public void setStatementsFinished(String out, boolean error) {
    synchronized (statementFinishedNotifier) {
      statementOutput = out;
      statementError = error;
      statementFinishedNotifier.notify();
    }
  }

  boolean pythonScriptInitialized = false;
  Integer pythonScriptInitializeNotifier = new Integer(0);

  public void onPythonScriptInitialized() {
    synchronized (pythonScriptInitializeNotifier) {
      pythonScriptInitialized = true;
      pythonScriptInitializeNotifier.notifyAll();
    }
  }

  public boolean isRunning() {
    return pythonscriptRunning;
  }


  @Override
  public void onProcessComplete(int exitValue) {
    pythonscriptRunning = false;
    logger.info("python process terminated. exit code " + exitValue);
  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    pythonscriptRunning = false;
    logger.error("python process failed", e);
  }

  public void close() {
    executor.getWatchdog().destroyProcess();
  }
}
