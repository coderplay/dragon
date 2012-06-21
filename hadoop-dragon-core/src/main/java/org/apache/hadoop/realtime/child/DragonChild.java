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
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.JobTokenIdentifier;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;

/**
 * The main() for Dragon task processes.
 */
class DragonChild {

  private static final Log LOG = LogFactory.getLog(DragonChild.class);

  static volatile TaskAttemptId taskid = null;

  public static void main(String[] args) throws Throwable {
    LOG.debug("Child starting");

    final Configuration defaultConf = new Configuration();
    defaultConf.addResource(DragonJobConfig.JOB_CONF_FILE);
    UserGroupInformation.setConfiguration(defaultConf);

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    final InetSocketAddress address = new InetSocketAddress(host, port);
    final String jobIdString = args[2];
    String containerIdString = args[3];

    // initialize metrics
    DefaultMetricsSystem.initialize(StringUtils.camelize("Task"));

    // TODO:Token<JobTokenIdentifier> jt = loadCredentials(defaultConf,
    // address);

    // Create DragonChildProtocol as actual task owner.
    UserGroupInformation taskOwner =
        UserGroupInformation.createRemoteUser(jobIdString);
    // taskOwner.addToken(jt);
    final DragonChildProtocol amProxy =
        taskOwner.doAs(new PrivilegedExceptionAction<DragonChildProtocol>() {
          @Override
          public DragonChildProtocol run() throws Exception {
            return instantiateAMProxy(defaultConf, address);
          }
        });

    // report non-pid to application master
    GetTaskRequest request = Records.newRecord(GetTaskRequest.class);
    request
        .setContainerId(DragonBuilderUtils.newContainerId(containerIdString));
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    ChildExecutionContext myContext = null;
    UserGroupInformation childUGI = null;
    try {
      int idleLoopCount = 0;
      // poll for new task
      for (int idle = 0; null == myContext; ++idle) {
        long sleepTimeMilliSecs = Math.min(idle * 500, 1500);
        LOG.info("Sleeping for " + sleepTimeMilliSecs
            + "ms before retrying again. Got null now.");
        MILLISECONDS.sleep(sleepTimeMilliSecs);
        myContext =
            ((GetTaskResponse) invoke(amProxy, "getTask", GetTaskRequest.class,
                request)).getTask();
      }
      DragonChild.taskid = myContext.getTaskAttemptId();

      // TODO:Create the job-conf and set credentials

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
      final ChildExecutionContext context = (ChildExecutionContext) myContext;
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
          Path workPath = new Path(workDir);
          ChildExecutor.run(defaultConf, amProxy, context); // run the task
          return null;
        }
      });
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      // amConnector.fsError(taskid, e.getMessage());
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      try {
        if (myContext != null) {
          // do cleanup for the task
          if (childUGI == null) { // no need to job into doAs block
            // TODO: myTask.taskCleanup(amConnector);
          } else {
            final ChildExecutionContext taskFinal = myContext;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                // TODO:taskFinal.taskCleanup(umbilical);
                return null;
              }
            });
          }
        }
      } catch (Exception e) {
        LOG.info("Exception cleaning up: " + StringUtils.stringifyException(e));
      }
      // Report back any failures, for diagnostic purposes
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      exception.printStackTrace(new PrintStream(baos));
      if (taskid != null) {
        // TODO:umbilical.fatalError(taskid, baos.toString());
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
          + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        Throwable tCause = throwable.getCause();
        String cause =
            tCause == null ? throwable.getMessage() : StringUtils
                .stringifyException(tCause);
        // TODO:umbilical.fatalError(taskid, cause);
      }
    } finally {
      RPC.stopProxy(amProxy);
      DefaultMetricsSystem.shutdown();
      // Shutting down log4j of the child-vm...
      // This assumes that on return from Task.run()
      // there is no more logging done.
      LogManager.shutdown();
    }
  }

  static DragonChildProtocol instantiateAMProxy(Configuration conf,
      final InetSocketAddress serviceAddr) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to ApplicationMaster at: " + serviceAddr);
    }
    YarnRPC rpc = YarnRPC.create(conf);
    DragonChildProtocol proxy =
        (DragonChildProtocol) rpc.getProxy(DragonChildProtocol.class,
            serviceAddr, conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connected to ApplicationMaster at: " + serviceAddr);
    }

    return proxy;
  }

  private synchronized static Object invoke(DragonChildProtocol realProxy,
      String method, Class argClass, Object args) throws YarnRemoteException {
    Method methodOb = null;
    try {
      methodOb = DragonChildProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException("Method name mismatch", e);
    }
    while (true) {
      try {
        return methodOb.invoke(realProxy, args);
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof YarnRemoteException) {
          LOG.warn("Error from remote end: "
              + e.getTargetException().getLocalizedMessage());
          if (LOG.isDebugEnabled())
            LOG.debug("Tracing remote error ", e.getTargetException());
          throw (YarnRemoteException) e.getTargetException();
        }
        if (LOG.isDebugEnabled())
          LOG.debug("Failed to contact AM for job " + taskid + " retrying..",
              e.getTargetException());
      } catch (Exception e) {
        if (LOG.isDebugEnabled())
          LOG.debug(
              "Failed to contact AM for job " + taskid + "  Will retry..", e);
      }
    }
  }

  private static Token<JobTokenIdentifier> loadCredentials(Configuration conf,
      InetSocketAddress address) throws IOException {
    // load token cache storage
    String tokenFileLocation =
        System.getenv(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME);
    String jobTokenFile =
        new Path(tokenFileLocation).makeQualified(FileSystem.getLocal(conf))
            .toUri().getPath();
    Credentials credentials = TokenCache.loadTokens(jobTokenFile, conf);
    LOG.debug("loading token. # keys =" + credentials.numberOfSecretKeys()
        + "; from file=" + jobTokenFile);
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    jt.setService(new Text(address.getAddress().getHostAddress() + ":"
        + address.getPort()));
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);
    for (Token<? extends TokenIdentifier> tok : credentials.getAllTokens()) {
      current.addToken(tok);
    }
    // Set the credentials
    // TODO:conf.setCredentials(credentials);
    return jt;
  }

}
