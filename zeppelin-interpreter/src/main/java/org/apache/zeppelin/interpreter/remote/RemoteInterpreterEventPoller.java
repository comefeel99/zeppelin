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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.thrift.TException;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer.ResourceKey;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEvent;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEventType;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.resource.ResourceInfo;
import org.apache.zeppelin.resource.ResourcePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RemoteInterpreterEventPoller extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(RemoteInterpreterEventPoller.class);

  private volatile boolean shutdown;

  private InterpreterConnectionFactory connectionFactory;
  private InterpreterGroup interpreterGroup;

  public RemoteInterpreterEventPoller() {
    shutdown = false;
  }

  public void setInterpreterProcess(InterpreterConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public void setInterpreterGroup(InterpreterGroup interpreterGroup) {
    this.interpreterGroup = interpreterGroup;
  }

  @Override
  public void run() {
    Client client = null;

    while (!shutdown) {
      // wait and retry
      if (!connectionFactory.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        continue;
      }

      try {
        client = connectionFactory.getClient();
      } catch (Exception e1) {
        logger.error("Can't get RemoteInterpreterEvent", e1);
        waitQuietly();
        continue;
      }

      RemoteInterpreterEvent event = null;
      try {
        event = client.getEvent();
      } catch (TException e) {
        logger.error("Can't get RemoteInterpreterEvent", e);
        waitQuietly();
        client.getOutputProtocol().getTransport().close();
        continue;
      } finally {
        connectionFactory.releaseClient(client);
      }

      Gson gson = new Gson();

      AngularObjectRegistry angularObjectRegistry = interpreterGroup.getAngularObjectRegistry();
      try {
        if (event.getType() == RemoteInterpreterEventType.NO_OP) {
          continue;
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_ADD) {
          AngularObject angularObject = gson.fromJson(event.getData(), AngularObject.class);
          angularObjectRegistry.add(angularObject.getName(),
              angularObject.get(), angularObject.getNoteId());
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_UPDATE) {
          AngularObject angularObject = gson.fromJson(event.getData(),
              AngularObject.class);
          AngularObject localAngularObject = angularObjectRegistry.get(
              angularObject.getName(), angularObject.getNoteId());
          if (localAngularObject instanceof RemoteAngularObject) {
            // to avoid ping-pong loop
            ((RemoteAngularObject) localAngularObject).set(
                angularObject.get(), true, false);
          } else {
            localAngularObject.set(angularObject.get());
          }
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_REMOVE) {
          AngularObject angularObject = gson.fromJson(event.getData(), AngularObject.class);
          angularObjectRegistry.remove(angularObject.getName(), angularObject.getNoteId());
        } else if (event.getType() == RemoteInterpreterEventType.RUN_INTERPRETER_CONTEXT_RUNNER) {
          InterpreterContextRunner runnerFromRemote = gson.fromJson(
              event.getData(), RemoteInterpreterContextRunner.class);

          connectionFactory.getInterpreterContextRunnerPool().run(
              runnerFromRemote.getNoteId(), runnerFromRemote.getParagraphId());
        } else if (event.getType() == RemoteInterpreterEventType.RESOURCE_POOL_SEARCH) {
          ResourceKey searchKey = gson.fromJson(event.data, ResourceKey.class);
          Collection<ResourceInfo> searchedInfo = searchResourceFromAllPool(
              searchKey.location,
              searchKey.name);

          // send search result back to interpreter process
          Client c = connectionFactory.getClient();

          try {
            c.resourcePoolInfo(
                searchKey.location,
                searchKey.name,
                gson.toJson(searchedInfo));
          } catch (TException e) {
            logger.error("error", e);
          } finally {
            connectionFactory.releaseClient(c);
          }
        } else if (event.getType() == RemoteInterpreterEventType.RESOURCE_POOL_GET) {
          ResourceKey searchKey = gson.fromJson(event.data, ResourceKey.class);
          ByteBuffer object = getResourceFromAllPool(searchKey.location, searchKey.name);

          // send search result back to interpreter process
          Client c = connectionFactory.getClient();

          try {
            c.resourcePoolObject(
                searchKey.location,
                searchKey.name,
                object);
          } catch (TException e) {
            logger.error("error", e);
          } finally {
            connectionFactory.releaseClient(c);
          }
        }
        logger.debug("Event from remoteproceess {}", event.getType());
      } catch (Exception e) {
        logger.error("Can't handle event " + event, e);
      }
    }
  }

  private ByteBuffer getResourceFromAllPool(String location, String name) {
    Gson gson = new Gson();
    List<InterpreterConnectionFactory> connectionFactoryToCheck =
        new LinkedList<InterpreterConnectionFactory>();

    synchronized (RemoteInterpreter.interpreterGroupReference) {
      for (InterpreterConnectionFactory connectionFactory :
        RemoteInterpreter.interpreterGroupReference.values()) {
        if (!connectionFactory.isRunning()) {
          continue;
        }
        connectionFactoryToCheck.add(connectionFactory);
      }
    }

    // if there're connectionFactory-resourcePoolId mapping, we don't need iterate all
    // connectionPool. that can be future improvements
    for (InterpreterConnectionFactory connectionFactory : connectionFactoryToCheck) {
      Client c;
      try {
        c = connectionFactory.getClient();
      } catch (Exception e1) {
        // just ignore the connection
        continue;
      }

      ByteBuffer object = null;
      try {
        Collection<ResourceInfo> infos = gson.fromJson(c.resourcePoolSearch(name),
            new TypeToken<Collection<ResourceInfo>>(){}.getType());

        for (ResourceInfo info : infos) {
          if (ResourcePool.match(location, name, info)) {
            object = c.resourcePoolGet(name);
            if (object != null) {
              return object;
            }
          }
        }
      } catch (TException e) {
        logger.error("error", e);
      } finally {
        connectionFactory.releaseClient(c);
      }
    }

    return null;
  }

  private Collection<ResourceInfo> searchResourceFromAllPool(String location, String name) {
    Gson gson = new Gson();
    List<InterpreterConnectionFactory> connectionFactoryToCheck =
        new LinkedList<InterpreterConnectionFactory>();

    synchronized (RemoteInterpreter.interpreterGroupReference) {
      for (InterpreterConnectionFactory connectionFactory :
        RemoteInterpreter.interpreterGroupReference.values()) {
        if (!connectionFactory.isRunning()) {
          continue;
        }
        connectionFactoryToCheck.add(connectionFactory);
      }
    }

    List<ResourceInfo> searchedInfo = new LinkedList<ResourceInfo>();

    // if there're connectionFactory-resourcePoolId mapping, we don't need iterate all
    // connectionPool. that can be future improvements
    for (InterpreterConnectionFactory connectionFactory : connectionFactoryToCheck) {
      Client c;
      try {
        c = connectionFactory.getClient();
      } catch (Exception e1) {
        // just ignore the connection
        continue;
      }

      try {
        Collection<ResourceInfo> infos = gson.fromJson(c.resourcePoolSearch(name),
            new TypeToken<Collection<ResourceInfo>>(){}.getType());

        for (ResourceInfo info : infos) {
          if (ResourcePool.match(location, name, info)) {
            searchedInfo.add(info);
          }
        }
      } catch (TException e) {
        logger.error("error", e);
      } finally {
        connectionFactory.releaseClient(c);
      }
    }

    return searchedInfo;
  }

  private void waitQuietly() {
    try {
      synchronized (this) {
        wait(1000);
      }
    } catch (InterruptedException ignored) {
    }
  }

  public void shutdown() {
    shutdown = true;
    synchronized (this) {
      notify();
    }
  }
}
