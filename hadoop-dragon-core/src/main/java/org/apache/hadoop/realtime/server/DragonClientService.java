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

package org.apache.hadoop.realtime.server;

import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.job.app.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskEvent;
import org.apache.hadoop.realtime.job.app.event.TaskEventType;
import org.apache.hadoop.realtime.protocol.DragonClientProtocol;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptResponse;
import org.apache.hadoop.realtime.protocol.records.GetCountersRequest;
import org.apache.hadoop.realtime.protocol.records.GetCountersResponse;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenRequest;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenResponse;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsRequest;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsResponse;
import org.apache.hadoop.realtime.protocol.records.GetJobReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetJobReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskAttemptReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskAttemptReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsResponse;
import org.apache.hadoop.realtime.protocol.records.KillJobRequest;
import org.apache.hadoop.realtime.protocol.records.KillJobResponse;
import org.apache.hadoop.realtime.protocol.records.KillTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.KillTaskAttemptResponse;
import org.apache.hadoop.realtime.protocol.records.KillTaskRequest;
import org.apache.hadoop.realtime.protocol.records.KillTaskResponse;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.security.authorize.DragonAMPolicyProvider;
import org.apache.hadoop.realtime.webapp.DragonWebApp;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.security.client.ClientTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.yarn.YarnException;
/**
 * This module is responsible for talking to the 
 * jobclient (user facing).
 *
 */
public class DragonClientService extends AbstractService 
    implements ClientService {

  static final Log LOG = LogFactory.getLog(DragonClientService.class);
  
  private DragonClientProtocol protocolHandler;
  private Server server;
  private WebApp webApp;
  private InetSocketAddress bindAddress;
  private AppContext appContext;

  public DragonClientService(AppContext appContext) {
    super("DragonClientService");
    this.appContext = appContext;
    this.protocolHandler = new DragonClientProtocolHandler();
  }

  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = NetUtils.createSocketAddr("0.0.0.0:0");
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }
    ClientToAMSecretManager secretManager = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new ClientToAMSecretManager();
      String secretKeyStr =
          System
              .getenv(ApplicationConstants.APPLICATION_CLIENT_SECRET_ENV_NAME);
      byte[] bytes = Base64.decodeBase64(secretKeyStr);
      ClientTokenIdentifier identifier = new ClientTokenIdentifier(
          this.appContext.getApplicationID());
      secretManager.setMasterKey(identifier, bytes);
    }
    server =
        rpc.getServer(DragonClientProtocol.class, protocolHandler, address,
            conf, secretManager,
            conf.getInt(DragonJobConfig.DRAGON_AM_JOB_CLIENT_THREAD_COUNT, 
                DragonJobConfig.DEFAULT_DRAGON_AM_JOB_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new DragonAMPolicyProvider());
    }

    server.start();
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress() + ":"
            + server.getPort());
    LOG.info("Instantiated DRAGONClientService at " + this.bindAddress);
    try {
      webApp =
          WebApps.$for("mapreduce", AppContext.class, appContext, "ws")
              .with(conf).start(new DragonWebApp());
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
    super.start();
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  public void stop() {
    server.stop();
    if (webApp != null) {
      webApp.stop();
    }
    super.stop();
  }

  @Override
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  @Override
  public int getHttpPort() {
    return webApp.port();
  }

  class DragonClientProtocolHandler implements DragonClientProtocol {

    private RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

    private Job verifyAndGetJob(JobId jobID, 
        boolean modifyAccess) throws YarnRemoteException {
      Job job = appContext.getJob(jobID);
      return job;
    }
 
    private Task verifyAndGetTask(TaskId taskID, 
        boolean modifyAccess) throws YarnRemoteException {
      Task task = verifyAndGetJob(taskID.getJobId(), 
          modifyAccess).getTask(taskID);
      if (task == null) {
        throw RPCUtil.getRemoteException("Unknown Task " + taskID);
      }
      return task;
    }

    private TaskAttempt verifyAndGetAttempt(TaskAttemptId attemptID, 
        boolean modifyAccess) throws YarnRemoteException {
      TaskAttempt attempt = verifyAndGetTask(attemptID.getTaskId(), 
          modifyAccess).getAttempt(attemptID);
      if (attempt == null) {
        throw RPCUtil.getRemoteException("Unknown TaskAttempt " + attemptID);
      }
      return attempt;
    }

    @Override
    public GetCountersResponse getCounters(GetCountersRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, false);
      GetCountersResponse response =
        recordFactory.newRecordInstance(GetCountersResponse.class);
      response.setCounters(job.getAllCounters());
      return response;
    }
    
    @Override
    public GetJobReportResponse getJobReport(GetJobReportRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      Job job = verifyAndGetJob(jobId, false);
      GetJobReportResponse response = 
        recordFactory.newRecordInstance(GetJobReportResponse.class);
      if (job != null) {
        response.setJobReport(job.getReport());
      }
      else {
        response.setJobReport(null);
      }
      return response;
    }

    @Override
    public GetTaskAttemptReportResponse getTaskAttemptReport(
        GetTaskAttemptReportRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      GetTaskAttemptReportResponse response =
        recordFactory.newRecordInstance(GetTaskAttemptReportResponse.class);
      response.setTaskAttemptReport(
          verifyAndGetAttempt(taskAttemptId, false).getReport());
      return response;
    }

    @Override
    public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) 
      throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      GetTaskReportResponse response = 
        recordFactory.newRecordInstance(GetTaskReportResponse.class);
      response.setTaskReport(verifyAndGetTask(taskId, false).getReport());
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillJobResponse killJob(KillJobRequest request) 
      throws YarnRemoteException {
      JobId jobId = request.getJobId();
      String message = "Kill Job received from client " + jobId;
      LOG.info(message);
  	  verifyAndGetJob(jobId, true);
      appContext.getEventHandler().handle(
          new JobDiagnosticsUpdateEvent(jobId, message));
      appContext.getEventHandler().handle(
          new JobEvent(jobId, JobEventType.JOB_KILL));
      KillJobResponse response = 
        recordFactory.newRecordInstance(KillJobResponse.class);
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public KillTaskResponse killTask(KillTaskRequest request) 
      throws YarnRemoteException {
      TaskId taskId = request.getTaskId();
      String message = "Kill task received from client " + taskId;
      LOG.info(message);
      verifyAndGetTask(taskId, true);
      appContext.getEventHandler().handle(
          new TaskEvent(taskId, TaskEventType.T_KILL));
      KillTaskResponse response = 
        recordFactory.newRecordInstance(KillTaskResponse.class);
      return response;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public KillTaskAttemptResponse killTaskAttempt(
        KillTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      String message = "Kill task attempt received from client " + taskAttemptId;
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, true);
      appContext.getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_KILL));
      KillTaskAttemptResponse response = 
        recordFactory.newRecordInstance(KillTaskAttemptResponse.class);
      return response;
    }

    @Override
    public GetDiagnosticsResponse getDiagnostics(
        GetDiagnosticsRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      
      GetDiagnosticsResponse response = 
        recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
      response.addAllDiagnostics(
          verifyAndGetAttempt(taskAttemptId, false).getDiagnostics());
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FailTaskAttemptResponse failTaskAttempt(
        FailTaskAttemptRequest request) throws YarnRemoteException {
      TaskAttemptId taskAttemptId = request.getTaskAttemptId();
      String message = "Fail task attempt received from client " + taskAttemptId;
      LOG.info(message);
      verifyAndGetAttempt(taskAttemptId, true);
      appContext.getEventHandler().handle(
          new TaskAttemptDiagnosticsUpdateEvent(taskAttemptId, message));
      appContext.getEventHandler().handle(
          new TaskAttemptEvent(taskAttemptId, 
              TaskAttemptEventType.TA_FAILMSG));
      FailTaskAttemptResponse response = recordFactory.
        newRecordInstance(FailTaskAttemptResponse.class);
      return response;
    }

    private final Object getTaskReportsLock = new Object();

    @Override
    public GetTaskReportsResponse getTaskReports(
        GetTaskReportsRequest request) throws YarnRemoteException {
      JobId jobId = request.getJobId();
      
      GetTaskReportsResponse response = 
        recordFactory.newRecordInstance(GetTaskReportsResponse.class);
      
      Job job = verifyAndGetJob(jobId, false);
      Collection<Task> tasks = job.getTasks().values();
      LOG.info("Getting task report for  " + jobId
          + ". Report-size will be " + tasks.size());

      // Take lock to allow only one call, otherwise heap will blow up because
      // of counters in the report when there are multiple callers.
      synchronized (getTaskReportsLock) {
        for (Task task : tasks) {
          response.addTaskReport(task.getReport());
        }
      }

      return response;
    }

    @Override
    public GetDelegationTokenResponse getDelegationToken(
        GetDelegationTokenRequest request) throws YarnRemoteException {
      throw RPCUtil.getRemoteException("DRAGON AM not authorized to issue delegation" +
      		" token");
    }
  }
}
