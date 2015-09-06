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
package org.apache.zeppelin.interpreter.helium;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.helium.Application;
import org.apache.zeppelin.helium.ApplicationArgument;
import org.apache.zeppelin.helium.ApplicationException;
import org.apache.zeppelin.helium.ApplicationKey;
import org.apache.zeppelin.helium.ApplicationSpec;
import org.apache.zeppelin.helium.Helium;
import org.apache.zeppelin.helium.HeliumConf;
import org.apache.zeppelin.helium.Signal;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.dev.ZeppelinApplicationDevServer;
import org.apache.zeppelin.resource.ResourceInfo;
import org.apache.zeppelin.resource.ResourceKey;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.resource.WellKnownResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeliumLauncher
 */
public class HeliumLauncher extends Application {
  Logger logger = LoggerFactory.getLogger(HeliumLauncher.class);

  public static boolean devMode = false;

  private Helium helium;

  private LinkedList<ApplicationSpec> availableApps;

  private Collection<ResourceInfo> searchResult;

  public HeliumLauncher(InterpreterContext context) {
    super(context);
    this.helium = Helium.singleton();
  }

  @Override
  protected void onChange(String name, Object oldObject, Object newObject) {
    InterpreterContext context = getInterpreterContext();

    if (name.equals("run")) {
      logger.info("Run {}", newObject);

      if (availableApps == null) {
        logger.error("no available app");
        return;
      }

      String run = (String) newObject;
      for (ApplicationSpec spec : availableApps) {
        if (spec.getClassName().equals(run)) {

          // get resource that application can consume
          for (ResourceInfo resource : searchResult) {
            if (helium.canConsume(spec, resource.name())) {
              logger.info("Load {} at {} with {}",
                  spec.getName(), resource.location(), resource.name());

              String location = resource.location();
              if (devMode) {
                location = context.getResourcePool().getId();
              }

              try {
                helium.load(spec,
                    location,
                    context.getNoteId(),
                    getPreviousParagraphId(),
                    new ApplicationArgument(new ResourceKey(resource.location(), resource.name())),
                    context);
              } catch (ApplicationException e) {
                logger.error("Error on loading " + spec.getName(), e);
              }
              break;
            }
          }
        }
      }

      bind("run", "");
    }
  }

  @Override
  public void signal(Signal signal) {
  }

  @Override
  public void load() throws IOException {
    InterpreterContext context = getInterpreterContext();
    context.out.write("%angular ");
    context.out.writeResource("interpreter/helium/HeliumLauncher.html");
  }

  private String getPreviousParagraphId() {
    InterpreterContext context = getInterpreterContext();
    InterpreterContextRunner previousParagraphRunner = null;

    List<InterpreterContextRunner> runners = context.getRunners();
    for (int i = 0; i < runners.size(); i++) {
      if (runners.get(i).getParagraphId().equals(context.getParagraphId())) {
        if (i > 0) {
          previousParagraphRunner = runners.get(i - 1);
        }
        break;
      }
    }

    if (previousParagraphRunner == null) {
      return null;
    }

    return previousParagraphRunner.getParagraphId();
  }

  @Override
  public void run(ApplicationArgument arg) throws IOException {
    // get previous paragraph's results

    InterpreterContext context = getInterpreterContext();
    String previousParagraphId = getPreviousParagraphId();

    ResourcePool pool = context.getResourcePool();

    searchResult = pool.search(WellKnownResource.resourceNameBelongsTo(
        context.getNoteId(), previousParagraphId));
    this.bind("resource", searchResult);

    availableApps = new LinkedList<ApplicationSpec>();
    for (ResourceInfo resource : searchResult) {
      Collection<ApplicationSpec> apps = helium.getAllApplicationsForResource(resource.name());
      if (apps == null || apps.isEmpty()) {
        continue;
      }

      for (ApplicationSpec app : apps) {
        if (!availableApps.contains(app)) {
          availableApps.add(app);
        }
      }
    }

    this.bind("availableApp", availableApps);

    // handler for button
    this.bind("run", "");
    this.watch("run");
  }

  @Override
  public void unload() throws ApplicationException {
  }


  /**
   * Dev mode
   */
  public static void main(String [] args) throws Exception {
    ZeppelinConfiguration conf = ZeppelinConfiguration.create();
    HeliumConf heliumConf = HeliumConf.create(new File(conf.getHeliumConfPath()));
    Helium helium = new Helium(heliumConf, conf.getHeliumLocalRepo());
    Helium.setSingleton(helium);
    HeliumLauncher.devMode = true;

    ZeppelinApplicationDevServer dev =
        new ZeppelinApplicationDevServer(HeliumLauncher.class.getName(), null);

    dev.server.start();
    dev.server.join();
  }
}
