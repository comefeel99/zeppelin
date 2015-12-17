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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.spark.PythonCompiler.PythonInterpretRequest;
import org.apache.zeppelin.spark.dep.DependencyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py4j.GatewayServer;

/**
 *
 */
public class PySparkInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(PySparkInterpreter.class);
  private GatewayServer gatewayServer;
  private int port;
  private String scriptPath;

  private Map<String, PythonCompiler> pythonCompilers = new HashMap<String, PythonCompiler>();

  static {
    Interpreter.register(
        "pyspark",
        "spark",
        PySparkInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
          .add("zeppelin.pyspark.python",
               SparkInterpreter.getSystemDefault("PYSPARK_PYTHON", null, "python"),
               "Python command to run pyspark with").build());
  }

  public PySparkInterpreter(Properties property) {
    super(property);

    scriptPath = System.getProperty("java.io.tmpdir") + "/zeppelin_pyspark.py";
  }

  private void createPythonScript() {
    ClassLoader classLoader = getClass().getClassLoader();
    File out = new File(scriptPath);

    if (out.exists() && out.isDirectory()) {
      throw new InterpreterException("Can't create python script " + out.getAbsolutePath());
    }

    try {
      FileOutputStream outStream = new FileOutputStream(out);
      IOUtils.copy(
          classLoader.getResourceAsStream("python/zeppelin_pyspark.py"),
          outStream);
      outStream.close();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    logger.info("File {} created", scriptPath);
  }

  @Override
  public void open() {
    DepInterpreter depInterpreter = getDepInterpreter();

    // load libraries from Dependency Interpreter
    URL [] urls = new URL[0];

    if (depInterpreter != null) {
      DependencyContext depc = depInterpreter.getDependencyContext();
      if (depc != null) {
        List<File> files = depc.getFiles();
        List<URL> urlList = new LinkedList<URL>();
        if (files != null) {
          for (File f : files) {
            try {
              urlList.add(f.toURI().toURL());
            } catch (MalformedURLException e) {
              logger.error("Error", e);
            }
          }

          urls = urlList.toArray(urls);
        }
      }
    }

    ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
    try {
      URLClassLoader newCl = new URLClassLoader(urls, oldCl);
      Thread.currentThread().setContextClassLoader(newCl);
      createGatewayServer();
    } catch (Exception e) {
      logger.error("Error", e);
      throw new InterpreterException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(oldCl);
    }
  }

  private void createGatewayServer() {
    // create python script
    createPythonScript();

    port = findRandomOpenPortOnAllLocalInterfaces();

    gatewayServer = new GatewayServer(this, port);
    gatewayServer.start();
  }

  private String getPythonCompilerId(InterpreterContext context) {
    return getSparkInterpreter().getScalaCompilerId(context);
  }

  private PythonCompiler createOrGetPythonCompiler(InterpreterContext context) {
    return createOrGetPythonCompiler(getPythonCompilerId(context));
  }
  private PythonCompiler createOrGetPythonCompiler(String compilerId) {

    synchronized (pythonCompilers) {
      if (!pythonCompilers.containsKey(compilerId)) {
        PythonCompiler pythonCompiler = new PythonCompiler();
        pythonCompiler.init(compilerId,
            getProperty("zeppelin.pyspark.python"),
            scriptPath, port,
            getSparkInterpreter().getSparkVersion().toNumber());
        pythonCompilers.put(compilerId, pythonCompiler);
      }

      return pythonCompilers.get(compilerId);
    }
  }

  private int findRandomOpenPortOnAllLocalInterfaces() {
    int port;
    try (ServerSocket socket = new ServerSocket(0);) {
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
    return port;
  }

  @Override
  public void close() {
    synchronized (pythonCompilers) {
      for (PythonCompiler c : pythonCompilers.values()) {
        c.close();
      }
      pythonCompilers.clear();
    }

    gatewayServer.shutdown();
  }



  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    if (sparkInterpreter.getSparkVersion().isUnsupportedVersion()) {
      return new InterpreterResult(Code.ERROR, "Spark "
          + sparkInterpreter.getSparkVersion().toString() + " is not supported");
    }

    PythonCompiler pythonCompiler = createOrGetPythonCompiler(context);
    return pythonCompiler.interpret(st, context, sparkInterpreter);
  }

  @Override
  public void cancel(InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    sparkInterpreter.cancel(context);
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    return sparkInterpreter.getProgress(context);
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    // not supported
    return new LinkedList<String>();
  }

  private SparkInterpreter getSparkInterpreter() {
    InterpreterGroup intpGroup = getInterpreterGroup();
    LazyOpenInterpreter lazy = null;
    SparkInterpreter spark = null;
    synchronized (intpGroup) {
      for (Interpreter intp : getInterpreterGroup()){
        if (intp.getClassName().equals(SparkInterpreter.class.getName())) {
          Interpreter p = intp;
          while (p instanceof WrappedInterpreter) {
            if (p instanceof LazyOpenInterpreter) {
              lazy = (LazyOpenInterpreter) p;
            }
            p = ((WrappedInterpreter) p).getInnerInterpreter();
          }
          spark = (SparkInterpreter) p;
        }
      }
    }
    if (lazy != null) {
      lazy.open();
    }
    return spark;
  }

  public ZeppelinContext getZeppelinContext(String compilerId) {
    SparkInterpreter sparkIntp = getSparkInterpreter();
    if (sparkIntp != null) {
      return getSparkInterpreter().createOrGetScalaCompiler(compilerId).getZeppelinContext();
    } else {
      return null;
    }
  }

  public JavaSparkContext getJavaSparkContext() {
    SparkInterpreter intp = getSparkInterpreter();
    if (intp == null) {
      return null;
    } else {
      return new JavaSparkContext(intp.getSparkContext());
    }
  }

  public SparkConf getSparkConf() {
    JavaSparkContext sc = getJavaSparkContext();
    if (sc == null) {
      return null;
    } else {
      return getJavaSparkContext().getConf();
    }
  }

  public SQLContext getSQLContext() {
    SparkInterpreter intp = getSparkInterpreter();
    if (intp == null) {
      return null;
    } else {
      return intp.getSQLContext();
    }
  }

  private DepInterpreter getDepInterpreter() {
    InterpreterGroup intpGroup = getInterpreterGroup();
    if (intpGroup == null) return null;
    synchronized (intpGroup) {
      for (Interpreter intp : intpGroup) {
        if (intp.getClassName().equals(DepInterpreter.class.getName())) {
          Interpreter p = intp;
          while (p instanceof WrappedInterpreter) {
            p = ((WrappedInterpreter) p).getInnerInterpreter();
          }
          return (DepInterpreter) p;
        }
      }
    }
    return null;
  }


  public PythonInterpretRequest getStatements(String compilerId) {
    PythonCompiler compiler = createOrGetPythonCompiler(compilerId);
    return compiler.getStatements();
  }

  public void setStatementsFinished(String compilerId, String out, boolean error) {
    PythonCompiler compiler = createOrGetPythonCompiler(compilerId);
    compiler.setStatementsFinished(out, error);
  }

  public void onPythonScriptInitialized(String compilerId) {
    PythonCompiler compiler = createOrGetPythonCompiler(compilerId);
    compiler.onPythonScriptInitialized();
  }
}
