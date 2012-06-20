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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.job.TaskInChildImpl;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.log4j.LogManager;

/**
 * The main() for MapReduce task processes.
 */
class YarnChild {

  private static final Log LOG = LogFactory.getLog(YarnChild.class);

  static volatile TaskAttemptId attemptId = null;

  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");

    final Configuration defaultConf = new Configuration();
    defaultConf.addResource(DragonJobConfig.JOB_CONF_FILE);

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress amAddress = new InetSocketAddress(host, port);
    final String jobIdString = args[2];
    final String containerIdString = args[3];

    final ChildServiceDelegate delegate =
        new ChildServiceDelegate(defaultConf, jobIdString, amAddress);

    // initialize metrics
    DefaultMetricsSystem.initialize(StringUtils.camelize("Task"));
    TaskInChild myTask = null;
    UserGroupInformation childUGI = null;
    try {
      for (int idle = 0; null == myTask; ++idle) {
        long sleepTimeMilliSecs = Math.min(idle * 500, 1500);
        LOG.info("Sleeping for " + sleepTimeMilliSecs
            + "ms before retrying again. Got null now.");
        MILLISECONDS.sleep(sleepTimeMilliSecs);
        myTask = delegate.getTask(containerIdString);
      }
      // TODO:YarnChild.attemptId = myTask.getID();

      // Initiate Java VM metrics
      JvmMetrics.initSingleton(containerIdString, "");
      childUGI =
          UserGroupInformation.createRemoteUser(System
              .getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      for (Token<?> token : UserGroupInformation.getCurrentUser().getTokens()) {
        childUGI.addToken(token);
      }

      // Create a final reference to the task for the doAs block
      final TaskInChildImpl taskFinal = new TaskInChildImpl(myTask);
      childUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // use job-specified working directory
          String workDir = defaultConf.get(DragonJobConfig.WORKING_DIR, "");
          if ("".equals(workDir)) {
            workDir =
                FileSystem.get(defaultConf).getWorkingDirectory().toString();
            defaultConf.set(DragonJobConfig.WORKING_DIR, workDir);
          }
          taskFinal.run(defaultConf, delegate); // run the task
          return null;
        }
      });
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      // delegate.fsError(attemptId, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (attemptId != null) {
        delegate.fatalError(attemptId, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
          + StringUtils.stringifyException(throwable));
      if (attemptId != null) {
        Throwable tCause = throwable.getCause();
        String cause =
            tCause == null ? throwable.getMessage() : StringUtils
                .stringifyException(tCause);
        delegate.fatalError(attemptId, cause);
      }
    } finally {
      delegate.stopProxy();
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }
}
