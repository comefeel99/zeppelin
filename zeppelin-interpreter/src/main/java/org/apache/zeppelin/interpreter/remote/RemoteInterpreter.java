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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.WrappedInterpreter;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterContext;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 *
 */
public class RemoteInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(RemoteInterpreter.class);
  Gson gson = new Gson();
  private String className;
  FormType formType;
  boolean initialized;

  static Map<String, InterpreterConnectionFactory> interpreterGroupReference
    = new HashMap<String, InterpreterConnectionFactory>();


  private InterpreterProcess interpreterProcess;

  public RemoteInterpreter(Properties property,
      String className,
      String interpreterRunner,
      String interpreterPath,
      int connectTimeout,
      String localRepo) {
    this(property, className, interpreterRunner, interpreterPath,
        new HashMap<String, String>(), connectTimeout, localRepo);
  }
  public RemoteInterpreter(Properties property,
      String className,
      String interpreterRunner,
      String interpreterPath,
      Map<String, String> env,
      int connectTimeout,
      String localRepo) {
    super(property);
    this.interpreterProcess = new LocalInterpreterProcess(
        interpreterRunner,
        interpreterPath,
        env,
        connectTimeout,
        localRepo);
    this.className = className;
  }
  public RemoteInterpreter(Properties property,
      String className,
      String host,
      int port) {
    super(property);
    this.className = className;
    this.interpreterProcess = new RemoteInterpreterProcess(host, port);
  }

  @Override
  public String getClassName() {
    return className;
  }

  public InterpreterConnectionFactory getInterpreterConnectionFactory() {
    synchronized (interpreterGroupReference) {
      if (interpreterGroupReference.containsKey(getInterpreterGroupKey(getInterpreterGroup()))) {
        InterpreterConnectionFactory connectionFactory = interpreterGroupReference
            .get(getInterpreterGroupKey(getInterpreterGroup()));
        try {
          return connectionFactory;
        } catch (Exception e) {
          throw new InterpreterException(e);
        }
      } else {
        throw new InterpreterException("Unexpected error");
      }
    }
  }

  private synchronized void init() {
    if (initialized == true) {
      return;
    }

    InterpreterConnectionFactory connectionFactory = null;

    synchronized (interpreterGroupReference) {
      if (interpreterGroupReference.containsKey(getInterpreterGroupKey(getInterpreterGroup()))) {
        connectionFactory = interpreterGroupReference
            .get(getInterpreterGroupKey(getInterpreterGroup()));
      } else {
        throw new InterpreterException("Unexpected error");
      }
    }

    int rc = connectionFactory.reference(getInterpreterGroup());

    synchronized (connectionFactory) {
      // when first process created
      if (rc == 1) {
        // create all interpreter class in this interpreter group
        Client client = null;
        try {
          client = connectionFactory.getClient();
        } catch (Exception e1) {
          throw new InterpreterException(e1);
        }

        try {
          for (Interpreter intp : this.getInterpreterGroup()) {
            logger.info("Create remote interpreter {}", intp.getClassName());
            client.createInterpreter(intp.getClassName(), (Map) property);

          }
        } catch (TException e) {
          throw new InterpreterException(e);
        } finally {
          connectionFactory.releaseClient(client);
        }
      }
    }
    initialized = true;
  }



  @Override
  public void open() {
    init();
  }

  @Override
  public void close() {
    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    connectionFactory.dereference();
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    FormType form = getFormType();
    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    Client client = null;
    try {
      client = connectionFactory.getClient();
    } catch (Exception e1) {
      throw new InterpreterException(e1);
    }

    InterpreterContextRunnerPool interpreterContextRunnerPool = connectionFactory
        .getInterpreterContextRunnerPool();

    List<InterpreterContextRunner> runners = context.getRunners();
    if (runners != null && runners.size() != 0) {
      // assume all runners in this InterpreterContext have the same note id
      String noteId = runners.get(0).getNoteId();

      interpreterContextRunnerPool.clear(noteId);
      interpreterContextRunnerPool.addAll(noteId, runners);
    }

    try {
      GUI settings = context.getGui();
      RemoteInterpreterResult remoteResult = client.interpret(className, st, convert(context));

      Map<String, Object> remoteConfig = (Map<String, Object>) gson.fromJson(
          remoteResult.getConfig(), new TypeToken<Map<String, Object>>() {
          }.getType());
      context.getConfig().clear();
      context.getConfig().putAll(remoteConfig);

      if (form == FormType.NATIVE) {
        GUI remoteGui = gson.fromJson(remoteResult.getGui(), GUI.class);
        context.getGui().clear();
        context.getGui().setParams(remoteGui.getParams());
        context.getGui().setForms(remoteGui.getForms());
      }

      InterpreterResult result = convert(remoteResult);
      return result;
    } catch (TException e) {
      throw new InterpreterException(e);
    } finally {
      connectionFactory.releaseClient(client);
    }
  }

  @Override
  public void cancel(InterpreterContext context) {
    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    Client client = null;
    try {
      client = connectionFactory.getClient();
    } catch (Exception e1) {
      throw new InterpreterException(e1);
    }

    try {
      client.cancel(className, convert(context));
    } catch (TException e) {
      throw new InterpreterException(e);
    } finally {
      connectionFactory.releaseClient(client);
    }
  }


  @Override
  public FormType getFormType() {
    init();

    if (formType != null) {
      return formType;
    }

    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    Client client = null;
    try {
      client = connectionFactory.getClient();
    } catch (Exception e1) {
      throw new InterpreterException(e1);
    }

    try {
      formType = FormType.valueOf(client.getFormType(className));
      return formType;
    } catch (TException e) {
      throw new InterpreterException(e);
    } finally {
      connectionFactory.releaseClient(client);
    }
  }

  @Override
  public int getProgress(InterpreterContext context) {
    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    Client client = null;
    try {
      client = connectionFactory.getClient();
    } catch (Exception e1) {
      throw new InterpreterException(e1);
    }

    try {
      return client.getProgress(className, convert(context));
    } catch (TException e) {
      throw new InterpreterException(e);
    } finally {
      connectionFactory.releaseClient(client);
    }
  }


  @Override
  public List<String> completion(String buf, int cursor) {
    InterpreterConnectionFactory connectionFactory = getInterpreterConnectionFactory();
    Client client = null;
    try {
      client = connectionFactory.getClient();
    } catch (Exception e1) {
      throw new InterpreterException(e1);
    }

    try {
      return client.completion(className, buf, cursor);
    } catch (TException e) {
      throw new InterpreterException(e);
    } finally {
      connectionFactory.releaseClient(client);
    }
  }

  @Override
  public Scheduler getScheduler() {
    int maxConcurrency = 10;
    // Share a single RemoteScheduler instance among all RemoteInterpreter in
    // the same InterpreterGroup.
    // If each RemoteInterpreter use each RemoteScheduler instance, can not guarantee
    // job submit sequence when Two or more interpreter shares single Scheduler.
    InterpreterGroup intpGroup = getInterpreterGroup();
    Interpreter firstInterpreter = intpGroup.get(0);
    Interpreter innerInterpreter = firstInterpreter;
    while (innerInterpreter instanceof WrappedInterpreter) {
      innerInterpreter = ((WrappedInterpreter) innerInterpreter).getInnerInterpreter();
    }

    if (innerInterpreter.equals(this)) {
      return SchedulerFactory.singleton().createOrGetRemoteScheduler(
          "remoteinterpreter_" + interpreterProcess.hashCode(),
          getInterpreterConnectionFactory(),
          maxConcurrency);
    } else {
      return innerInterpreter.getScheduler();
    }
  }


  @Override
  public void setInterpreterGroup(InterpreterGroup interpreterGroup) {
    super.setInterpreterGroup(interpreterGroup);

    synchronized (interpreterGroupReference) {
      InterpreterConnectionFactory connectionFactory = interpreterGroupReference
          .get(getInterpreterGroupKey(interpreterGroup));

      // when interpreter process is not created or terminated
      if (connectionFactory == null || !connectionFactory.isRunning()) {
        interpreterGroupReference.put(getInterpreterGroupKey(interpreterGroup),
              new InterpreterConnectionFactory(interpreterProcess));
      }
      logger.info("setInterpreterGroup = "
          + getInterpreterGroupKey(interpreterGroup) + " class=" + className);
    }
  }

  private String getInterpreterGroupKey(InterpreterGroup interpreterGroup) {
    return interpreterGroup.getId();
  }

  public static RemoteInterpreterContext convert(InterpreterContext ic) {
    Gson gson = new Gson();
    return new RemoteInterpreterContext(
        ic.getNoteId(),
        ic.getParagraphId(),
        ic.getParagraphTitle(),
        ic.getParagraphText(),
        gson.toJson(ic.getConfig()),
        gson.toJson(ic.getGui()),
        gson.toJson(ic.getRunners()));
  }

  public static InterpreterResult convert(RemoteInterpreterResult result) {
    return new InterpreterResult(
        InterpreterResult.Code.valueOf(result.getCode()),
        Type.valueOf(result.getType()),
        result.getMsg());
  }
}
