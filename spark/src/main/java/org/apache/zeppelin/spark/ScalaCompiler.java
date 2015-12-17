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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.repl.SparkIMain;
import org.apache.spark.repl.SparkJLineCompletion;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.spark.dep.DependencyResolver;

import scala.None;
import scala.Some;
import scala.tools.nsc.Settings;

/**
 * Scala compiler instance
 */
public class ScalaCompiler {
  private SparkILoop interpreter;
  private SparkIMain intp;
  private ByteArrayOutputStream out;
  private SparkJLineCompletion completor;
  private Map<String, Object> binder;
  private PrintStream printStream;
  private ZeppelinContext z;
  private boolean initialized = false;
  private Settings settings;
  private DependencyResolver dep;

  public ScalaCompiler(Settings settings) {
    this.settings = settings;

    out = new ByteArrayOutputStream();
    printStream = new PrintStream(out);
    interpreter = new SparkILoop(null, new PrintWriter(out));
    interpreter.settings_$eq(settings);
    interpreter.createInterpreter();
    intp = interpreter.intp();
    intp.setContextClassLoader();
    intp.initializeSynchronous();
    completor = new SparkJLineCompletion(intp);
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void init(SparkContext sc,
      SQLContext sqlc,
      int maxResult,
      String localRepoPath,
      String additionalRemoteRepository) {
    SparkVersion sparkVersion = SparkVersion.fromVersionString(sc.version());
    dep = new DependencyResolver(intp, sc, localRepoPath, additionalRemoteRepository);

    z = new ZeppelinContext(sc, sqlc, null, dep, printStream, maxResult);

    intp.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
    binder = (Map<String, Object>) getValue("_binder");
    binder.put("sc", sc);
    binder.put("sqlc", sqlc);
    binder.put("z", z);
    binder.put("out", printStream);

    intp.interpret("@transient val z = "
                 + "_binder.get(\"z\").asInstanceOf[org.apache.zeppelin.spark.ZeppelinContext]");
    intp.interpret("@transient val sc = "
                 + "_binder.get(\"sc\").asInstanceOf[org.apache.spark.SparkContext]");
    intp.interpret("@transient val sqlc = "
                 + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
    intp.interpret("@transient val sqlContext = "
                 + "_binder.get(\"sqlc\").asInstanceOf[org.apache.spark.sql.SQLContext]");
    intp.interpret("import org.apache.spark.SparkContext._");

    if (sparkVersion.oldSqlContextImplicits()) {
      intp.interpret("import sqlContext._");
    } else {
      intp.interpret("import sqlContext.implicits._");
      intp.interpret("import sqlContext.sql");
      intp.interpret("import org.apache.spark.sql.functions._");
    }


    /* Temporary disabling DisplayUtils. see https://issues.apache.org/jira/browse/ZEPPELIN-127
     *
    // Utility functions for display
    intp.interpret("import org.apache.zeppelin.spark.utils.DisplayUtils._");

    // Scala implicit value for spark.maxResult
    intp.interpret("import org.apache.zeppelin.spark.utils.SparkMaxResult");
    intp.interpret("implicit val sparkMaxResult = new SparkMaxResult(" +
            Integer.parseInt(getProperty("zeppelin.spark.maxResult")) + ")");
     */

    try {
      if (sparkVersion.oldLoadFilesMethodName()) {
        Method loadFiles = this.interpreter.getClass().getMethod("loadFiles", Settings.class);
        loadFiles.invoke(this.interpreter, settings);
      } else {
        Method loadFiles = this.interpreter.getClass().getMethod(
                "org$apache$spark$repl$SparkILoop$$loadFiles", Settings.class);
        loadFiles.invoke(this.interpreter, settings);
      }
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
            | IllegalArgumentException | InvocationTargetException e) {
      throw new InterpreterException(e);
    }


    initialized = true;
  }

  public SparkILoop getInterpreter() {
    return interpreter;
  }

  public SparkIMain getIntp() {
    return intp;
  }

  public ByteArrayOutputStream getOut() {
    return out;
  }

  public SparkJLineCompletion getCompletor() {
    return completor;
  }

  public Map<String, Object> getBinder() {
    return binder;
  }

  public PrintStream getPrintStream() {
    return printStream;
  }

  private Object getValue(String name) {
    Object ret = intp.valueOfTerm(name);
    if (ret instanceof None) {
      return null;
    } else if (ret instanceof Some) {
      return ((Some) ret).get();
    } else {
      return ret;
    }
  }

  public ZeppelinContext getZeppelinContext() {
    return z;
  }


}
