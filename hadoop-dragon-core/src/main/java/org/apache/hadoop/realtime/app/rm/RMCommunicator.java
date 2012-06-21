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

package org.apache.hadoop.realtime.app.rm;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.server.ClientService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.Records;

/**
 * Registers/unregisters to RM and sends heartbeats to RM.
 */
public abstract class RMCommunicator extends AbstractService  {
  private static final Log LOG = LogFactory.getLog(RMCommunicator.class);
  private int rmPollInterval;//millis
  protected ApplicationId applicationId;
  protected ApplicationAttemptId applicationAttemptId;
  private AtomicBoolean stopped;
  protected Thread allocatorThread;
  @SuppressWarnings("rawtypes")
  protected EventHandler eventHandler;
  protected AMRMProtocol scheduler;
  protected int lastResponseID;
  private Resource minContainerCapability;
  private Resource maxContainerCapability;
  protected Map<ApplicationAccessType, String> applicationACLs;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final AppContext context;
  private Job job;

  public RMCommunicator(AppContext context) {
    super("RMCommunicator");
    this.context = context;
    this.eventHandler = context.getEventHandler();
    this.applicationId = context.getApplicationID();
    this.applicationAttemptId = context.getApplicationAttemptId();
    this.stopped = new AtomicBoolean(false);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rmPollInterval =
        conf.getInt(DragonJobConfig.DRAGON_AM_TO_RM_HEARTBEAT_INTERVAL_MS,
            DragonJobConfig.DEFAULT_DRAGON_AM_TO_RM_HEARTBEAT_INTERVAL_MS);
  }

  @Override
  public void start() {
    scheduler= createSchedulerProxy();
    register();
    startAllocatorThread();
    JobId jobId = Records.newRecord(JobId.class);
    jobId.setAppId(applicationId);
    jobId.setId(applicationId.getId());
    job = context.getJob(jobId);
    super.start();
  }

  protected AppContext getContext() {
    return context;
  }

  protected Job getJob() {
    return job;
  }

  protected void register() {
    //Register
    String host = context.getClientServiceAddress().getAddress()
        .getCanonicalHostName();
    try {
      RegisterApplicationMasterRequest request =
        recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      request.setHost(host);
      request.setRpcPort(context.getClientServiceAddress().getPort());
      request.setTrackingUrl(host + ":" + context.getClientServiceHttpPort());
      RegisterApplicationMasterResponse response =
        scheduler.registerApplicationMaster(request);
      minContainerCapability = response.getMinimumResourceCapability();
      maxContainerCapability = response.getMaximumResourceCapability();
      this.applicationACLs = response.getApplicationACLs();
      LOG.info("minContainerCapability: " + minContainerCapability.getMemory());
      LOG.info("maxContainerCapability: " + maxContainerCapability.getMemory());
    } catch (Exception are) {
      LOG.error("Exception while registering", are);
      throw new YarnException(are);
    }
  }

  protected void unregister() {
    try {
      FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
      if (job.getState() == JobState.KILLED) {
        finishState = FinalApplicationStatus.KILLED;
      } else if (job.getState() == JobState.FAILED
          || job.getState() == JobState.ERROR) {
        finishState = FinalApplicationStatus.FAILED;
      }
      StringBuffer sb = new StringBuffer();
      for (String s : job.getDiagnostics()) {
        sb.append(s).append("\n");
      }
      LOG.info("Setting job diagnostics to " + sb.toString());

      FinishApplicationMasterRequest request =
          recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      request.setAppAttemptId(this.applicationAttemptId);
      request.setFinishApplicationStatus(finishState);
      request.setDiagnostics(sb.toString());
      scheduler.finishApplicationMaster(request);
    } catch(Exception are) {
      LOG.error("Exception while unregistering ", are);
    }
  }

  protected Resource getMinContainerCapability() {
    return minContainerCapability;
  }

  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  @Override
  public void stop() {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException while stopping", ie);
    }
    unregister();
    super.stop();
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              heartbeat();
            } catch (YarnException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              // TODO: for other exceptions
            }
          } catch (InterruptedException e) {
            LOG.warn("Allocated thread interrupted. Returning.");
            return;
          }
        }
      }
    });
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  protected AMRMProtocol createSchedulerProxy() {
    final YarnRPC rpc = YarnRPC.create(getConfig());
    final Configuration conf = getConfig();
    final String serviceAddr = conf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is " + tokenURLEncodedStr);
      }
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
      }
    });
  }

  protected abstract void heartbeat() throws Exception;
}
