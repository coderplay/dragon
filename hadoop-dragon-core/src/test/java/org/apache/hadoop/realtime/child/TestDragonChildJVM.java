/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.realtime.child;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncher;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncherEvent;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.server.DragonApp;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.junit.Assert;
import org.junit.Test;

public class TestDragonChildJVM {
  
  private static final Log LOG = LogFactory.getLog(TestDragonChildJVM.class);

  @Test
  public void testCommandLine() throws Exception {
    MyDragonApp app = new MyDragonApp(1, true, this.getClass().getName(), true);
    Job job = app.submit(new Configuration());
    app.waitForState(job, JobState.FAILED);
    app.verifyCompleted();

    Assert.assertEquals("[exec $JAVA_HOME/bin/java"
        + " -Djava.net.preferIPv4Stack=true"
        + " -Dhadoop.metrics.log.level=WARN"
        + "  -Xmx200m -Djava.io.tmpdir=$PWD/tmp"
        + " -Dlog4j.configuration=dragon-container-log4j.properties"
        + " -Dyarn.app.dragon.container.log.dir=<LOG_DIR>"
        + " -Dyarn.app.dragon.container.log.filesize=0"
        + " -Dhadoop.root.logger=INFO,CLA"
        + " org.apache.hadoop.realtime.child.DragonChild 127.0.0.1 " + "54321"
        + " job_0_0000 " + "container_0_0000_01_000000" + " 1><LOG_DIR>/stdout"
        + " 2><LOG_DIR>/stderr ]", app.myCommandLine);
  }

  private static final class MyDragonApp extends DragonApp {

    private String myCommandLine;

    public MyDragonApp(int tasks, boolean autoComplete, String testName,
        boolean cleanOnStart) {
      super(tasks, autoComplete, testName, cleanOnStart);
    }

    @Override
    protected ContainerLauncher createContainerLauncher(AppContext context) {
      return new MockContainerLauncher() {
        @Override
        public void handle(ContainerLauncherEvent event) {
          if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {
            ContainerRemoteLaunchEvent launchEvent =
                (ContainerRemoteLaunchEvent) event;
            ContainerLaunchContext launchContext = launchEvent.getContainer();
            String cmdString = launchContext.getCommands().toString();
            LOG.info("launchContext " + cmdString);
            myCommandLine = cmdString;
          }
          super.handle(event);
        }
      };
    }
  }
}
