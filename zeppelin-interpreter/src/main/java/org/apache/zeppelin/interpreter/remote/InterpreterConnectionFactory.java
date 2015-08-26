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

import com.google.gson.Gson;

import org.apache.commons.exec.*;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Get/Release client connection to interpreter process
 */
public class InterpreterConnectionFactory implements ExecuteResultHandler {
  private static final Logger logger = LoggerFactory.getLogger(InterpreterConnectionFactory.class);
  private final int GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 30 * 1000;

  private final AtomicInteger referenceCount;

  private GenericObjectPool<Client> clientPool;
  private final RemoteInterpreterEventPoller remoteInterpreterEventPoller;
  private final InterpreterContextRunnerPool interpreterContextRunnerPool;

  private final InterpreterProcess interpreterProcess;

  public InterpreterConnectionFactory(InterpreterProcess interpreterProcess) {
    this(interpreterProcess, new RemoteInterpreterEventPoller());
  }

  InterpreterConnectionFactory(InterpreterProcess interpreterProcess,
      RemoteInterpreterEventPoller remoteInterpreterEventPoller) {
    this.interpreterProcess = interpreterProcess;
    this.interpreterContextRunnerPool = new InterpreterContextRunnerPool();
    referenceCount = new AtomicInteger(0);
    this.remoteInterpreterEventPoller = remoteInterpreterEventPoller;
  }

  public InterpreterProcess getInterpreterProcess() {
    return interpreterProcess;
  }

  public int reference(InterpreterGroup interpreterGroup) {
    synchronized (referenceCount) {
      if (!interpreterProcess.isRunning()) {
        interpreterProcess.start();
      }

      if (clientPool == null) {
        clientPool = new GenericObjectPool<Client>(new ClientFactory(
            interpreterProcess.getHost(), interpreterProcess.getPort()));

        remoteInterpreterEventPoller.setInterpreterGroup(interpreterGroup);
        remoteInterpreterEventPoller.setInterpreterProcess(this);
        remoteInterpreterEventPoller.start();
      }

      return referenceCount.incrementAndGet();
    }
  }

  public Client getClient() throws Exception {
    return clientPool.borrowObject();
  }

  public void releaseClient(Client client) {
    clientPool.returnObject(client);
  }

  public int dereference() {
    synchronized (referenceCount) {
      int r = referenceCount.decrementAndGet();
      if (r == 0) {
        logger.info("shutdown interpreter process");
        remoteInterpreterEventPoller.shutdown();

        // first try shutdown
        try {
          Client client = getClient();
          client.shutdown();
          releaseClient(client);
        } catch (Exception e) {
          logger.error("Error", e);
        }

        clientPool.clear();
        clientPool.close();

        // wait for some time (GRACEFUL_SHUTDOWN_TIMEOUT_SEC) and force kill
        // remote process server.serve() loop is not always finishing gracefully
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < GRACEFUL_SHUTDOWN_TIMEOUT_SEC) {
          if (interpreterProcess.isRunning()) {
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
            }
          } else {
            break;
          }
        }

        if (interpreterProcess.isRunning()) {
          logger.info("kill interpreter process");
          interpreterProcess.stop();
        }

        logger.info("Remote process terminated");
      }
      return r;
    }
  }

  public int referenceCount() {
    synchronized (referenceCount) {
      return referenceCount.get();
    }
  }

  @Override
  public void onProcessComplete(int exitValue) {
    logger.info("Interpreter process exited {}", exitValue);
  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    logger.info("Interpreter process failed {}", e);
  }

  public int getNumActiveClient() {
    if (clientPool == null) {
      return 0;
    } else {
      return clientPool.getNumActive();
    }
  }

  public int getNumIdleClient() {
    if (clientPool == null) {
      return 0;
    } else {
      return clientPool.getNumIdle();
    }
  }

  /**
   * Called when angular object is updated in client side to propagate
   * change to the remote process
   * @param name
   * @param o
   */
  public void updateRemoteAngularObject(String name, String noteId, Object o) {
    Client client = null;
    try {
      client = getClient();
    } catch (NullPointerException e) {
      // remote process not started
      return;
    } catch (Exception e) {
      logger.error("Can't update angular object", e);
    }

    try {
      Gson gson = new Gson();
      client.angularObjectUpdate(name, noteId, gson.toJson(o));
    } catch (TException e) {
      logger.error("Can't update angular object", e);
    } finally {
      releaseClient(client);
    }
  }

  public InterpreterContextRunnerPool getInterpreterContextRunnerPool() {
    return interpreterContextRunnerPool;
  }

  public boolean isRunning() {
    return interpreterProcess.isRunning();
  }
}
