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
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectRegistryListener;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.helium.Application;
import org.apache.zeppelin.helium.ApplicationArgument;
import org.apache.zeppelin.helium.ApplicationException;
import org.apache.zeppelin.helium.ApplicationKey;
import org.apache.zeppelin.helium.ApplicationLoader;
import org.apache.zeppelin.helium.Helium;
import org.apache.zeppelin.interpreter.ClassloaderInterpreter;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.data.TableData;
import org.apache.zeppelin.interpreter.thrift.ApplicationResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterContext;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEvent;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEventType;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResult;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService;
import org.apache.zeppelin.resource.ByteBufferInputStream;
import org.apache.zeppelin.resource.ResourceInfo;
import org.apache.zeppelin.resource.ResourceKey;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.resource.ResourcePoolEventHandler;
import org.apache.zeppelin.resource.WellKnownResource;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.scheduler.JobListener;
import org.apache.zeppelin.scheduler.JobProgressPoller;
import org.apache.zeppelin.scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 *
 */
public class RemoteInterpreterServer extends Thread implements RemoteInterpreterService.Iface,
    AngularObjectRegistryListener, ResourcePoolEventHandler {
  Logger logger = LoggerFactory.getLogger(RemoteInterpreterServer.class);

  InterpreterGroup interpreterGroup;
  AngularObjectRegistry angularObjectRegistry;
  ResourcePool resourcePool;
  Gson gson = new Gson();

  RemoteInterpreterService.Processor<RemoteInterpreterServer> processor;
  RemoteInterpreterServer handler;
  private int port;
  private TThreadPoolServer server;

  ApplicationLoader appLoader;

  List<RemoteInterpreterEvent> eventQueue = new LinkedList<RemoteInterpreterEvent>();

  public RemoteInterpreterServer(int port, String localRepo) throws TTransportException {
    this.port = port;
    interpreterGroup = new InterpreterGroup();
    angularObjectRegistry = new AngularObjectRegistry(interpreterGroup.getId(), this);
    resourcePool = new ResourcePool(this);
    interpreterGroup.setAngularObjectRegistry(angularObjectRegistry);

    appLoader = createAppLoader(localRepo);
    interpreterGroup.setAppLoader(appLoader);

    processor = new RemoteInterpreterService.Processor<RemoteInterpreterServer>(this);
    TServerSocket serverTransport = new TServerSocket(port);
    server = new TThreadPoolServer(
        new TThreadPoolServer.Args(serverTransport).processor(processor));
  }

  private ApplicationLoader createAppLoader(String localRepo) {
    // create app loader
    ClassLoader cl = getClass().getClassLoader();
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }

    return new ApplicationLoader(cl, new DependencyResolver(localRepo));
  }

  @Override
  public void run() {
    logger.info("Starting remote interpreter server on port {}", port);
    server.serve();
  }

  @Override
  public void shutdown() throws TException {
    interpreterGroup.close();
    interpreterGroup.destroy();

    server.stop();

    // server.stop() does not always finish server.serve() loop
    // sometimes server.serve() is hanging even after server.stop() call.
    // this case, need to force kill the process
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 2000 && server.isServing()) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
      }
    }

    if (server.isServing()) {
      System.exit(0);
    }
  }

  public int getPort() {
    return port;
  }

  public boolean isRunning() {
    if (server == null) {
      return false;
    } else {
      return server.isServing();
    }
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    String localRepo = args[1];
    RemoteInterpreterServer remoteInterpreterServer =
        new RemoteInterpreterServer(port, localRepo);
    remoteInterpreterServer.start();
    remoteInterpreterServer.join();
    System.exit(0);
  }

  @Override
  public void createInterpreter(String className, Map<String, String> properties)
      throws TException {
    try {
      Class<Interpreter> replClass = (Class<Interpreter>) Object.class.forName(className);
      Properties p = new Properties();
      p.putAll(properties);

      Constructor<Interpreter> constructor = replClass
          .getConstructor(new Class[] { Properties.class });
      Interpreter repl = constructor.newInstance(p);

      ClassLoader cl = ClassLoader.getSystemClassLoader();
      repl.setClassloaderUrls(new URL[] {});

      synchronized (interpreterGroup) {
        interpreterGroup.add(new LazyOpenInterpreter(new ClassloaderInterpreter(repl, cl)));
      }
      logger.info("Instantiate interpreter {}", className);
      repl.setInterpreterGroup(interpreterGroup);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
        | InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      e.printStackTrace();
      throw new TException(e);
    }
  }

  protected Interpreter getInterpreter(String className) throws TException {
    synchronized (interpreterGroup) {
      for (Interpreter inp : interpreterGroup) {
        if (inp.getClassName().equals(className)) {
          return inp;
        }
      }
    }
    throw new TException(new InterpreterException("Interpreter instance " + className
        + " not found"));
  }

  public InterpreterGroup getInterpreterGroup() {
    return interpreterGroup;
  }

  @Override
  public void open(String className) throws TException {
    Interpreter intp = getInterpreter(className);
    intp.open();
  }

  @Override
  public void close(String className) throws TException {
    Interpreter intp = getInterpreter(className);
    intp.close();
  }

  @Override
  public RemoteInterpreterResult interpret(String className, String st,
      RemoteInterpreterContext interpreterContext) throws TException {
    Interpreter intp = getInterpreter(className);
    InterpreterContext context = convert(interpreterContext);

    // clear previous result
    Collection<ResourceInfo> previousResult = resourcePool.search(
        context.getResourcePool().getId(),
        WellKnownResource.resourceNameBelongsTo(context.getNoteId(), context.getParagraphId()));
    if (previousResult != null) {
      for (ResourceInfo info : previousResult) {
        logger.info("remove previous result {}", info.name());
        resourcePool.remove(info.name());
      }
    }

    Scheduler scheduler = intp.getScheduler();
    InterpretJobListener jobListener = new InterpretJobListener();
    InterpretJob job = new InterpretJob(interpreterContext.getParagraphId(),
        interpreterContext.getParagraphId(), jobListener, JobProgressPoller.DEFAULT_INTERVAL_MSEC,
        intp, st, context);

    scheduler.submit(job);

    while (!job.isTerminated()) {
      synchronized (jobListener) {
        try {
          jobListener.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    InterpreterResult result;
    if (job.getStatus() == Status.ERROR) {
      result = new InterpreterResult(Code.ERROR, Job.getStack(job.getException()));
    } else {
      result = (InterpreterResult) job.getReturn();
    }

    if (result.type() == Type.TABLE) {
      resourcePool.put(
          WellKnownResource.resourceName(
              WellKnownResource.TABLE_DATA,
              WellKnownResource.INSTANCE_RESULT,
              context.getNoteId(), context.getParagraphId()),
          new TableData(result));
    }
    return convert(result, context.getConfig(), context.getGui());
  }

  class InterpretJobListener implements JobListener {

    @Override
    public void onProgressUpdate(Job job, int progress) {
    }

    @Override
    public void beforeStatusChange(Job job, Status before, Status after) {
    }

    @Override
    public void afterStatusChange(Job job, Status before, Status after) {
      synchronized (this) {
        notifyAll();
      }
    }
  }

  class InterpretJob extends Job {

    private Interpreter interpreter;
    private String script;
    private InterpreterContext context;

    public InterpretJob(String jobId, String jobName, JobListener listener,
        long progressUpdateIntervalMsec, Interpreter interpreter, String script,
        InterpreterContext context) {
      super(jobId, jobName, listener, progressUpdateIntervalMsec);
      this.interpreter = interpreter;
      this.script = script;
      this.context = context;
    }

    @Override
    public int progress() {
      return 0;
    }

    @Override
    public Map<String, Object> info() {
      return null;
    }

    @Override
    protected Object jobRun() throws Throwable {
      InterpreterResult result = interpreter.interpret(script, context);

      // add InterpreterOutput a head of the message
      String output = null;

      byte[] interpreterOutput = context.out.toByteArray(clearInterpreterOutput());
      if (interpreterOutput != null) {
        output = new String(interpreterOutput);
      }

      if (result.message() != null) {
        if (output == null || output.length() == 0) {
          output = result.toString();
        } else {
          output += result.message();
        }
      }

      return new InterpreterResult(result.code(), output);
    }

    @Override
    protected boolean jobAbort() {
      return false;
    }
  }

  protected boolean clearInterpreterOutput() {
    return true;
  }

  @Override
  public void cancel(String className, RemoteInterpreterContext interpreterContext)
      throws TException {
    Interpreter intp = getInterpreter(className);
    intp.cancel(convert(interpreterContext));
  }

  @Override
  public int getProgress(String className, RemoteInterpreterContext interpreterContext)
      throws TException {
    Interpreter intp = getInterpreter(className);
    return intp.getProgress(convert(interpreterContext));
  }

  @Override
  public String getFormType(String className) throws TException {
    Interpreter intp = getInterpreter(className);
    return intp.getFormType().toString();
  }

  @Override
  public List<String> completion(String className, String buf, int cursor) throws TException {
    Interpreter intp = getInterpreter(className);
    return intp.completion(buf, cursor);
  }

  private InterpreterContext convert(RemoteInterpreterContext ric) {
    List<InterpreterContextRunner> contextRunners = new LinkedList<InterpreterContextRunner>();
    List<InterpreterContextRunner> runners = gson.fromJson(ric.getRunners(),
        new TypeToken<List<RemoteInterpreterContextRunner>>() {
        }.getType());

    for (InterpreterContextRunner r : runners) {
      contextRunners.add(new ParagraphRunner(this, r.getNoteId(), r.getParagraphId()));
    }

    return new InterpreterContext(ric.getNoteId(), ric.getParagraphId(), ric.getParagraphTitle(),
        ric.getParagraphText(), (Map<String, Object>) gson.fromJson(ric.getConfig(),
            new TypeToken<Map<String, Object>>() {
            }.getType()), gson.fromJson(ric.getGui(), GUI.class),
        interpreterGroup.getAngularObjectRegistry(), contextRunners, createInterpreterOutput(),
        resourcePool);
  }

  protected InterpreterOutput createInterpreterOutput() {
    return new InterpreterOutput();
  }

  static class ParagraphRunner extends InterpreterContextRunner {

    private transient RemoteInterpreterServer server;

    public ParagraphRunner(RemoteInterpreterServer server, String noteId, String paragraphId) {
      super(noteId, paragraphId);
      this.server = server;
    }

    @Override
    public void run() {
      Gson gson = new Gson();
      server.sendEvent(new RemoteInterpreterEvent(
          RemoteInterpreterEventType.RUN_INTERPRETER_CONTEXT_RUNNER, gson.toJson(this)));
    }
  }

  private RemoteInterpreterResult convert(InterpreterResult result, Map<String, Object> config,
      GUI gui) {
    return new RemoteInterpreterResult(result.code().name(), result.type().name(),
        result.message(), gson.toJson(config), gson.toJson(gui));
  }

  @Override
  public String getStatus(String jobId) throws TException {
    synchronized (interpreterGroup) {
      for (Interpreter intp : interpreterGroup) {
        for (Job job : intp.getScheduler().getJobsRunning()) {
          if (jobId.equals(job.getId())) {
            return job.getStatus().name();
          }
        }

        for (Job job : intp.getScheduler().getJobsWaiting()) {
          if (jobId.equals(job.getId())) {
            return job.getStatus().name();
          }
        }
      }
    }
    return "Unknown";
  }

  @Override
  public void onAdd(String interpreterGroupId, AngularObject object) {
    sendEvent(new RemoteInterpreterEvent(RemoteInterpreterEventType.ANGULAR_OBJECT_ADD,
        gson.toJson(object)));
  }

  @Override
  public void onUpdate(String interpreterGroupId, AngularObject object) {
    sendEvent(new RemoteInterpreterEvent(RemoteInterpreterEventType.ANGULAR_OBJECT_UPDATE,
        gson.toJson(object)));
  }

  @Override
  public void onRemove(String interpreterGroupId, String name, String noteId, String paragraphId) {
    Map<String, String> removeObject = new HashMap<String, String>();
    removeObject.put("name", name);
    removeObject.put("noteId", noteId);
    removeObject.put("paragraphId", paragraphId);

    sendEvent(new RemoteInterpreterEvent(RemoteInterpreterEventType.ANGULAR_OBJECT_REMOVE,
        gson.toJson(removeObject)));
  }

  public void sendEvent(RemoteInterpreterEvent event) {
    synchronized (eventQueue) {
      eventQueue.add(event);
      eventQueue.notifyAll();
    }
  }

  @Override
  public RemoteInterpreterEvent getEvent() throws TException {
    synchronized (eventQueue) {
      if (eventQueue.isEmpty()) {
        try {
          eventQueue.wait(1000);
        } catch (InterruptedException e) {
        }
      }

      if (eventQueue.isEmpty()) {
        return new RemoteInterpreterEvent(RemoteInterpreterEventType.NO_OP, "");
      } else {
        return eventQueue.remove(0);
      }
    }
  }

  /**
   * called when object is updated in client (web) side.
   *
   * @param className
   * @param name
   * @param noteId
   *          noteId where the update issues
   * @param object
   * @throws TException
   */
  @Override
  public void angularObjectUpdate(String name, String noteId, String paragraphId, String object)
      throws TException {
    AngularObjectRegistry registry = interpreterGroup.getAngularObjectRegistry();
    // first try local objects
    AngularObject ao = registry.get(name, noteId, paragraphId);
    if (ao == null) {
      logger.error("Angular object {} not exists", name);
      return;
    }

    if (object == null) {
      ao.set(null, false);
      return;
    }

    Object oldObject = ao.get();
    Object value = null;
    if (oldObject != null) { // first try with previous object's type
      try {
        value = gson.fromJson(object, oldObject.getClass());
        ao.set(value, false);
        return;
      } catch (Exception e) {
        // no luck
      }
    }

    // Generic java object type for json.
    if (value == null) {
      try {
        value = gson.fromJson(object, new TypeToken<Map<String, Object>>() {
        }.getType());
      } catch (Exception e) {
        // no lock
      }
    }

    // try string object type at last
    if (value == null) {
      value = gson.fromJson(object, String.class);
    }

    ao.set(value, false);
  }

  /**
   * When zeppelinserver initiate angular object add. Dont't need to emit event
   * to zeppelin server
   */
  @Override
  public void angularObjectAdd(String name, String noteId, String paragraphId, String object)
      throws TException {
    AngularObjectRegistry registry = interpreterGroup.getAngularObjectRegistry();
    // first try local objects
    AngularObject ao = registry.get(name, noteId, paragraphId);
    if (ao != null) {
      angularObjectUpdate(name, noteId, paragraphId, object);
      return;
    }

    // Generic java object type for json.
    Object value = null;
    try {
      value = gson.fromJson(object, new TypeToken<Map<String, Object>>() {
      }.getType());
    } catch (Exception e) {
      // nolock
    }

    // try string object type at last
    if (value == null) {
      value = gson.fromJson(object, String.class);
    }

    registry.add(name, value, noteId, paragraphId, false);
  }

  @Override
  public void angularObjectRemove(String name, String noteId, String paragraphId)
      throws TException {
    AngularObjectRegistry registry = interpreterGroup.getAngularObjectRegistry();
    registry.remove(name, noteId, paragraphId, false);
  }

  // Resource pool -----

  List<ResourceCall> resourcePoolsearchEventQueue = new LinkedList<ResourceCall>();
  List<ResourceCall> resourcePoolgetEventQueue = new LinkedList<ResourceCall>();

  @Override
  public void resourcePoolInfo(String location, String namePattern, String object)
      throws TException {
    ResourceCall r = null;

    logger.info("Search result for location {}, name {} received", location, namePattern);
    synchronized (resourcePoolsearchEventQueue) {
      ResourceCall key = new ResourceCall(location, namePattern);
      int i = resourcePoolsearchEventQueue.indexOf(key);
      if (i < 0) {
        // not found. ignore event
        logger.warn("Got search result for location={}, name={}, but no handler found", location,
            namePattern);
      } else {
        r = resourcePoolsearchEventQueue.remove(i);
      }
    }

    if (r != null) {
      synchronized (r) {
        r.object = object;
        r.notified = true;
        r.notify();
      }
    }
  }

  @Override
  public void resourcePoolObject(String location, String name, ByteBuffer byteBuffer)
      throws TException {
    ResourceCall r = null;

    synchronized (resourcePoolgetEventQueue) {
      ResourceCall key = new ResourceCall(location, name);
      int i = resourcePoolgetEventQueue.indexOf(key);
      if (i < 0) {
        // not found. ignore event
        logger.warn("Got resource from pool location={}, name={}, but no handler found", location,
            name);
      } else {
        r = resourcePoolgetEventQueue.remove(i);
      }
    }

    if (r != null) {
      Object object = null;
      try {
        object = ResourcePool.deserializeResource(byteBuffer);
      } catch (ClassNotFoundException | IOException e) {
        logger.error("error", e);
      }

      synchronized (r) {
        r.object = object;
        r.notified = true;
        r.notify();
      }
    }
  }

  /**
   * ZeppelinServer searches local pool
   */
  @Override
  public String resourcePoolSearch(String namePattern) throws TException {
    Collection<ResourceInfo> infos = resourcePool.search(resourcePool.getId(), namePattern);
    if (infos == null) {
      return null;
    } else {
      return gson.toJson(infos);
    }
  }

  /**
   * ZeppelinServer Get object from local pool
   */
  @Override
  public ByteBuffer resourcePoolGet(String name) throws TException {
    Object o = resourcePool.get(resourcePool.getId(), name);

    try {
      return ResourcePool.serializeResource(o);
    } catch (IOException e) {
      logger.error("error", e);
    }
    return null;
  }

  @Override
  public Collection<ResourceInfo> resourcePoolSearch(String location, String namePattern) {
    ResourceCall r = new ResourceCall(location, namePattern);
    RemoteInterpreterEvent event = new RemoteInterpreterEvent(
        RemoteInterpreterEventType.RESOURCE_POOL_SEARCH, gson.toJson(r));

    synchronized (resourcePoolsearchEventQueue) {
      resourcePoolsearchEventQueue.add(r);
    }

    sendEvent(event);

    synchronized (r) {
      if (!r.notified) {
        try {
          r.wait(60 * 1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (r.object == null) {
      // search timeout
      logger.error("Search timeout");
      return null;
    } else {
      return gson.fromJson((String) r.object, new TypeToken<Collection<ResourceInfo>>() {
      }.getType());
    }
  }

  @Override
  public Object resourcePoolGetObject(String location, String name) {
    ResourceCall r = new ResourceCall(location, name);
    RemoteInterpreterEvent event = new RemoteInterpreterEvent(
        RemoteInterpreterEventType.RESOURCE_POOL_GET, gson.toJson(r));

    synchronized (resourcePoolgetEventQueue) {
      resourcePoolgetEventQueue.add(r);
    }

    sendEvent(event);

    synchronized (r) {
      if (!r.notified) {
        try {
          r.wait(60 * 1000);
        } catch (InterruptedException e) {
        }
      }
    }

    return r.object;
  }

  static class ResourceCall extends ResourceKey {
    public Object object = null;
    public boolean notified;

    public ResourceCall(String location, String name) {
      super(location, name);
      notified = false;
    }
  }


  @Override
  public String getResourcePoolId() throws TException {
    return resourcePool.getId();
  }

  public ResourcePool getResourcePool() {
    return resourcePool;
  }

  /**
   * ZeppelinServer -> Interpreter
   */
  @Override
  public ApplicationResult loadApplication(String artifact, String classname,
      String noteId, String paragraphId,
      String inputResourceLocation,
      String inputResourceName,
      RemoteInterpreterContext interpreterContext) throws TException {
    InterpreterContext context = convert(interpreterContext);

    try {
      Helium.loadLocal(
          new ApplicationKey(artifact, classname),
          noteId,
          paragraphId,
          resourcePool,
          new ApplicationArgument(new ResourceKey(inputResourceLocation, inputResourceName)),
          context,
          appLoader
      );
    } catch (ApplicationException e) {
      logger.error("Error on run application " + classname, e);
      return new ApplicationResult(1, e.getMessage());
    }

    try {
      return new ApplicationResult(0, new String(context.out.toByteArray()));
    } catch (IOException e) {
      logger.error("Error on run application " + classname, e);
      return new ApplicationResult(1, e.getMessage());
    }
  }

  /**
   * ZeppelinServer -> Interpreter
   */
  @Override
  public int unloadApplication(String artifact, String classname,
      String noteId, String paragraphId) throws TException {

    try {
      Helium.unloadLocal(
          new ApplicationKey(artifact, classname), noteId, paragraphId, resourcePool);
    } catch (ApplicationException e) {
      logger.error("Error on unload application " + classname, e);
      return 1;
    }

    return 0;
  }
}
