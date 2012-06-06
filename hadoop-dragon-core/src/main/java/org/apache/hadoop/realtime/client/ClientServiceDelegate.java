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

package org.apache.hadoop.realtime.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.ResourceMgrDelegate;
import org.apache.hadoop.realtime.protocol.DragonClientProtocol;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsRequest;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsResponse;
import org.apache.hadoop.realtime.protocol.records.GetJobReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetJobReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsResponse;
import org.apache.hadoop.realtime.protocol.records.KillJobRequest;
import org.apache.hadoop.realtime.protocol.records.KillTaskAttemptRequest;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;

public class ClientServiceDelegate {
  private static final Log LOG = LogFactory.getLog(ClientServiceDelegate.class);

  // Caches for per-user NotRunningJobs
  private HashMap<JobState, HashMap<String, NotRunningJob>> notRunningJobs;

  private final Configuration conf;
  private final JobId jobId;
  private final ApplicationId appId;
  private final ResourceMgrDelegate rm;
  private DragonClientProtocol realProxy = null;
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private static String UNKNOWN_USER = "Unknown User";
  private String trackingUrl;

  private boolean amAclDisabledStatusLogged = false;

  public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm,
      JobId jobId) {
    this.conf = new Configuration(conf); // Cloning for modifying.
    // For faster redirects from AM to HS.
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        this.conf.getInt(DragonJobConfig.DRAGON_CLIENT_TO_AM_IPC_MAX_RETRIES,
            DragonJobConfig.DEFAULT_DRAGON_CLIENT_TO_AM_IPC_MAX_RETRIES));
    this.rm = rm;
    this.jobId = jobId;
    this.appId = jobId.getAppId();
    notRunningJobs = new HashMap<JobState, HashMap<String, NotRunningJob>>();
  }

  // Get the instance of the NotRunningJob corresponding to the specified
  // user and state
  private NotRunningJob getNotRunningJob(ApplicationReport applicationReport,
      JobState state) {
    synchronized (notRunningJobs) {
      HashMap<String, NotRunningJob> map = notRunningJobs.get(state);
      if (map == null) {
        map = new HashMap<String, NotRunningJob>();
        notRunningJobs.put(state, map);
      }
      String user =
          (applicationReport == null) ?
              UNKNOWN_USER : applicationReport.getUser();
      NotRunningJob notRunningJob = map.get(user);
      if (notRunningJob == null) {
        notRunningJob = new NotRunningJob(applicationReport, state);
        map.put(user, notRunningJob);
      }
      return notRunningJob;
    }
  }

  private DragonClientProtocol getProxy() throws YarnRemoteException {
    if (realProxy != null) {
      return realProxy;
    }
    ApplicationReport application = rm.getApplicationReport(appId);
    if (application != null) {
      trackingUrl = application.getTrackingUrl();
    }
    String serviceAddr = null;
    while (application == null
        || YarnApplicationState.RUNNING == application
            .getYarnApplicationState()) {
      try {
        if (application.getHost() == null || "".equals(application.getHost())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("AM not assigned to Job. Waiting to get the AM ...");
          }
          Thread.sleep(2000);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Application state is "
                + application.getYarnApplicationState());
          }
          application = rm.getApplicationReport(appId);
          continue;
        }
        if(!conf.getBoolean(DragonJobConfig.JOB_AM_ACCESS_DISABLED, false)) {
          UserGroupInformation newUgi = UserGroupInformation.createRemoteUser(
              UserGroupInformation.getCurrentUser().getUserName());
          serviceAddr = application.getHost() + ":" + application.getRpcPort();
          if (UserGroupInformation.isSecurityEnabled()) {
            String clientTokenEncoded = application.getClientToken();
            Token<ApplicationTokenIdentifier> clientToken =
              new Token<ApplicationTokenIdentifier>();
            clientToken.decodeFromUrlString(clientTokenEncoded);
            // RPC layer client expects ip:port as service for tokens
            InetSocketAddress addr = NetUtils.createSocketAddr(application
                .getHost(), application.getRpcPort());
            clientToken.setService(new Text(addr.getAddress().getHostAddress()
                + ":" + addr.getPort()));
            newUgi.addToken(clientToken);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to " + serviceAddr);
          }
          final String tempStr = serviceAddr;
          realProxy = newUgi.doAs(new PrivilegedExceptionAction<DragonClientProtocol>() {
            @Override
            public DragonClientProtocol run() throws IOException {
              return instantiateAMProxy(tempStr);
            }
          });
        } else {
          if (!amAclDisabledStatusLogged) {
            LOG.info("Network ACL closed to AM for job " + jobId
                + ". Not going to try to reach the AM.");
            amAclDisabledStatusLogged = true;
          }
          return getNotRunningJob(null, JobState.RUNNING);
        }
        return realProxy;
      } catch (IOException e) {
        //possibly the AM has crashed
        //there may be some time before AM is restarted
        //keep retrying by getting the address from RM
        LOG.info("Could not connect to " + serviceAddr +
        ". Waiting for getting the latest AM address...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          LOG.warn("getProxy() call interruped", e1);
          throw new YarnException(e1);
        }
        application = rm.getApplicationReport(appId);
      } catch (InterruptedException e) {
        LOG.warn("getProxy() call interruped", e);
        throw new YarnException(e);
      }
    }

    /** we just want to return if its allocating, so that we don't
     * block on it. This is to be able to return job status
     * on an allocating Application.
     */
    String user = application.getUser();
    if (user == null) {
      throw RPCUtil.getRemoteException("User is not set in the application report");
    }
    if (application.getYarnApplicationState() == YarnApplicationState.NEW ||
        application.getYarnApplicationState() == YarnApplicationState.SUBMITTED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.NEW);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.FAILED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.FAILED);
    }

    if (application.getYarnApplicationState() == YarnApplicationState.KILLED) {
      realProxy = null;
      return getNotRunningJob(application, JobState.KILLED);
    }

    return realProxy;
  }


  DragonClientProtocol instantiateAMProxy(final String serviceAddr)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to ApplicationMaster at: " + serviceAddr);
    }
    YarnRPC rpc = YarnRPC.create(conf);
    DragonClientProtocol proxy = 
         (DragonClientProtocol) rpc.getProxy(DragonClientProtocol.class,
            NetUtils.createSocketAddr(serviceAddr), conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connected to ApplicationMaster at: " + serviceAddr);
    }
    return proxy;
  }

  private synchronized Object invoke(String method, Class argClass,
      Object args) throws YarnRemoteException {
    Method methodOb = null;
    try {
      methodOb = DragonClientProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException("Method name mismatch", e);
    }
    while (true) {
      try {
        return methodOb.invoke(getProxy(), args);
      } catch (YarnRemoteException yre) {
        LOG.warn("Exception thrown by remote end.", yre);
        throw yre;
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof YarnRemoteException) {
          LOG.warn("Error from remote end: " + e
              .getTargetException().getLocalizedMessage());
          LOG.debug("Tracing remote error ", e.getTargetException());
          throw (YarnRemoteException) e.getTargetException();
        }
        LOG.debug("Failed to contact AM/History for job " + jobId + 
            " retrying..", e.getTargetException());
        // Force reconnection by setting the proxy to null.
        realProxy = null;
      } catch (Exception e) {
        LOG.debug("Failed to contact AM/History for job " + jobId
            + "  Will retry..", e);
        // Force reconnection by setting the proxy to null.
        realProxy = null;
      }
    }
  }

  public String[] getTaskDiagnostics(TaskAttemptId attemptId)
      throws IOException, InterruptedException {
    GetDiagnosticsRequest request = recordFactory
        .newRecordInstance(GetDiagnosticsRequest.class);
    request.setTaskAttemptId(attemptId);
    List<String> list = ((GetDiagnosticsResponse) invoke("getDiagnostics",
        GetDiagnosticsRequest.class, request)).getDiagnosticsList();
    String[] result = new String[list.size()];
    int i = 0;
    for (String c : list) {
      result[i++] = c.toString();
    }
    return result;
  }
  
  public JobReport getJobReport(JobId jobId) throws YarnRemoteException {
    GetJobReportRequest request =
        recordFactory.newRecordInstance(GetJobReportRequest.class);
    request.setJobId(jobId);
    JobReport report = ((GetJobReportResponse) invoke("getJobReport",
        GetJobReportRequest.class, request)).getJobReport();
    return report;
  }

  public List<TaskReport> getTaskReports(JobId jobId)
       throws YarnRemoteException, YarnRemoteException {
    GetTaskReportsRequest request =
        recordFactory.newRecordInstance(GetTaskReportsRequest.class);
    request.setJobId(jobId);

    List<TaskReport> taskReports =
      ((GetTaskReportsResponse) invoke("getTaskReports", GetTaskReportsRequest.class,
          request)).getTaskReportList();

    return taskReports;
  }

  public boolean killTask(TaskAttemptId taskAttemptId, boolean fail)
       throws YarnRemoteException {
    if (fail) {
      FailTaskAttemptRequest failRequest = recordFactory.newRecordInstance(FailTaskAttemptRequest.class);
      failRequest.setTaskAttemptId(taskAttemptId);
      invoke("failTaskAttempt", FailTaskAttemptRequest.class, failRequest);
    } else {
      KillTaskAttemptRequest killRequest = recordFactory.newRecordInstance(KillTaskAttemptRequest.class);
      killRequest.setTaskAttemptId(taskAttemptId);
      invoke("killTaskAttempt", KillTaskAttemptRequest.class, killRequest);
    }
    return true;
  }

  public boolean killJob(JobId jobId)
       throws YarnRemoteException {
    KillJobRequest killRequest = recordFactory.newRecordInstance(KillJobRequest.class);
    killRequest.setJobId(jobId);
    invoke("killJob", KillJobRequest.class, killRequest);
    return true;
  }
}
