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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.helium.Application;
import org.apache.zeppelin.helium.ApplicationArgument;
import org.apache.zeppelin.helium.ApplicationException;
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

  private final String APP_TO_RUN = "launcherRun";              // app class name to run
  private final String SHOW = "launcherShow";                     // show, hide, loading
  private final String AVAILABLE_APPS = "launcherAvailableApps";
  private InterpreterContext context;

  public HeliumLauncher() {
    this.helium = Helium.singleton();
  }

  @Override
  protected void onChange(String name, Object oldObject, Object newObject) {

    if (name.equals(APP_TO_RUN)) {
      logger.info("Run {}", newObject);

      this.put(context, SHOW, "hide");
      refresh();
    }
  }

  @Override
  public void signal(Signal signal) {
  }

  private void refresh() {
    for (InterpreterContextRunner runner : context.getRunners()) {
      if (context.getParagraphId().equals(runner.getParagraphId())) {
        // refresh
        logger.info("refresh");
        runner.run();
        break;
      }
    }
  }

  @Override
  public void load() throws IOException {
  }


  @Override
  public void run(ApplicationArgument arg, InterpreterContext context) throws IOException {
    this.context = context;

    context.out.setHeader("%angular ");
    context.out.writeResource("interpreter/helium/HeliumLauncher.html");

    // get previous paragraph's results
    String previousParagraphId = getPreviousParagraphId(context);

    Collection<ResourceInfo> searchResult;
    if (HeliumLauncher.devMode) {
      ResourcePool pool = context.getResourcePool();
      searchResult = pool.search(WellKnownResource.resourceNameBelongsTo(
          context.getNoteId(), previousParagraphId));
    } else {
      searchResult = ResourcePool.searchAll(WellKnownResource.resourceNameBelongsTo(
          context.getNoteId(), previousParagraphId));
    }


    String applicationToRun = (String) get(context, APP_TO_RUN);
    LinkedList<ApplicationSpec> availableApps;
    if (applicationToRun == null || applicationToRun.isEmpty()) {  // show launcher
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

      this.put(context, SHOW, "show");
      this.put(context, APP_TO_RUN, "");
      this.watch(context, APP_TO_RUN);
      put(context, AVAILABLE_APPS, availableApps);
    } else { // load and run app
      ApplicationSpec spec = null;
      for (ApplicationSpec s : helium.getAllSpecs()) {
        if (s.getClassName().equals(applicationToRun)) {
          spec = s;
          break;
        }
      }

      // get resource that application can consume
      for (ResourceInfo resource : searchResult) {
        if (helium.canConsume(spec, resource.name())) {
          logger.info("Load {} at {} with {}",
              spec.getName(), resource.location(), resource.name());

          String location = resource.location();
          if (HeliumLauncher.devMode) {
            location = context.getResourcePool().getId();
          }

          helium.load(spec,
              location,
              context.getNoteId(),
              previousParagraphId,
              new ApplicationArgument(new ResourceKey(resource.location(), resource.name())),
              context);
        }
      }
    }
  }

  @Override
  public void unload() throws ApplicationException {
  }



  private String getPreviousParagraphId(InterpreterContext context) {
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
