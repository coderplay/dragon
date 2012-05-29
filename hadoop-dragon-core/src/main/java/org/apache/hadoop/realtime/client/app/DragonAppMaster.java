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
package org.apache.hadoop.realtime.client.app;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.DragonClientService;
import org.apache.hadoop.realtime.job.IJobInApp;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.server.JobInApplicationMaster;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class DragonAppMaster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(DragonAppMaster.class);
  private Clock clock;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptId;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;

  private JobId jobId;
  private IJobInApp job;
  private JobEventDispatcher jobEventDispatcher;

  private DragonClientService clientService;

  private AppContext context;
  private Dispatcher dispatcher;
  private UserGroupInformation currentUser;
  private Credentials fsTokens = new Credentials(); // Filled during init

  // Para used to connect ResourceManager
  private Configuration conf;
  private YarnRPC rpc;
  private AMRMProtocol resourceManager;

  // Para used to regist ResourceManager
  private ApplicationAttemptId appAttemptID;
  private String appMasterHostname = "";
  private int appMasterRpcPort = 1;
  private String appMasterTrackingUrl = "";

  public DragonAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime);
  }

  public DragonAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime) {
    super(DragonAppMaster.class.getName());
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptId = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    LOG.info("Created DragonAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void init(final Configuration conf) {

    // Get all needed security tokens.
    downloadTokensAndSetupUGI(conf);

    // Initialize application context,name,attemptId,jobId
    context = new RunningAppContext(conf);
    appName = conf.get(DragonJobConfig.JOB_NAME, "<missing app name>");
    conf.setInt(DragonJobConfig.APPLICATION_ATTEMPT_ID,
        appAttemptId.getAttemptId());

    jobId =
        DragonBuilderUtils.newJobId(appAttemptId.getApplicationId(),
            appAttemptId.getApplicationId().getId());

    // service to hand out event
    dispatcher = createDispatcher();
    addIfService(dispatcher);

    // service to handle requests to TaskUmbilicalProtocol
    /*
     * taskAttemptListener = createTaskAttemptListener(context);
     * addIfService(taskAttemptListener);
     */

    // service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

    // Initialize the JobEventDispatcher
    this.jobEventDispatcher = new JobEventDispatcher();

    // register the event dispatchers
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    /*
     * dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
     * dispatcher.register(TaskAttemptEventType.class, new
     * TaskAttemptEventDispatcher());
     * 
     * // service to allocate containers from RM containerAllocator =
     * createContainerAllocator(clientService, context);
     * addIfService(containerAllocator);
     * dispatcher.register(ContainerAllocator.EventType.class,
     * containerAllocator);
     * 
     * // corresponding service to launch allocated containers via NodeManager
     * containerLauncher = createContainerLauncher(context);
     * addIfService(containerLauncher);
     * dispatcher.register(ContainerLauncher.EventType.class,
     * containerLauncher);
     */

    // Mock something about ResourceManager
    rpc = YarnRPC.create(conf);
    appAttemptID = Records.newRecord(ApplicationAttemptId.class);
    appAttemptID = ConverterUtils.toApplicationAttemptId("");
    resourceManager = connectToRM();

    try {
      RegisterApplicationMasterResponse response = registerToRM();

    } catch (YarnRemoteException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    super.init(conf);
  } // end of init()

  @Override
  public void start() {
    job = createJob(getConfig());
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    jobEventDispatcher.handle(initJobEvent);
    super.start();
  }

  /**
   * Initial DragonAppMaster Get and CheckOut necessary parameters from system
   * environment eg: container_Id,host,port,http_port,submitTime
   * 
   * @param args
   */
  public static void main(String[] args) {
    try {
      String containerIdStr =
          System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
      String nodeHostString = System.getenv(ApplicationConstants.NM_HOST_ENV);
      String nodePortString = System.getenv(ApplicationConstants.NM_PORT_ENV);
      String nodeHttpPortString =
          System.getenv(ApplicationConstants.NM_HTTP_PORT_ENV);
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);

      validateInputParam(containerIdStr,
          ApplicationConstants.AM_CONTAINER_ID_ENV);
      validateInputParam(nodeHostString, ApplicationConstants.NM_HOST_ENV);
      validateInputParam(nodePortString, ApplicationConstants.NM_PORT_ENV);
      validateInputParam(nodeHttpPortString,
          ApplicationConstants.NM_HTTP_PORT_ENV);
      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();
      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      DragonAppMaster appMaster =
          new DragonAppMaster(applicationAttemptId, containerId,
              nodeHostString, Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime);
      Runtime.getRuntime().addShutdownHook(
          new CompositeServiceShutdownHook(appMaster));
      YarnConfiguration conf = new YarnConfiguration();
      conf.addResource(new Path(DragonJobConfig.JOB_CONF_FILE));
      String jobUserName =
          System.getenv(ApplicationConstants.Environment.USER.name());
      conf.set(DragonJobConfig.USER_NAME, jobUserName);
      initAndStartAppMaster(appMaster, conf, jobUserName);

    } catch (Throwable t) {
      LOG.error("Error starting DragonAppMaster", t);
      System.exit(1);
    }
  }

  protected static void initAndStartAppMaster(final DragonAppMaster appMaster,
      final YarnConfiguration conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi =
        UserGroupInformation.createRemoteUser(jobUserName);
    appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        appMaster.init(conf);
        appMaster.start();
        return null;
      }
    });
  }

  private static void validateInputParam(String value, String param)
      throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /** Create and initialize (but don't start) a single job. */
  protected IJobInApp createJob(Configuration conf) {
    // create single job
    IJobInApp newJob =
        new JobInApplicationMaster(jobId, appAttemptId, conf,
            dispatcher.getEventHandler(), fsTokens, clock,
            currentUser.getUserName(), appSubmitTime);
    ((RunningAppContext) context).jobs.put(newJob.getID(), newJob);
    return newJob;
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  private class RunningAppContext implements AppContext {

    private final Map<JobId, IJobInApp> jobs =
        new ConcurrentHashMap<JobId, IJobInApp>();
    private final Configuration conf;

    public RunningAppContext(Configuration config) {
      this.conf = config;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptId;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appAttemptId.getApplicationId();
    }

    @Override
    public String getApplicationName() {
      return appName;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public IJobInApp getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, IJobInApp> getAllJobs() {
      return jobs;
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public CharSequence getUser() {
      return this.conf.get(DragonJobConfig.USER_NAME);
    }

    @Override
    public Clock getClock() {
      return clock;
    }
  }

  /**
   * Obtain the tokens needed by the job and put them in the UGI
   * 
   * @param conf
   */
  protected void downloadTokensAndSetupUGI(Configuration conf) {
    try {
      this.currentUser = UserGroupInformation.getCurrentUser();
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read the file-system tokens from the localized tokens-file.
        Path jobSubmitDir =
            FileContext.getLocalFSFileContext().makeQualified(
                new Path(new File(DragonJobConfig.JOB_SUBMIT_DIR)
                    .getAbsolutePath()));
        Path jobTokenFile =
            new Path(jobSubmitDir, DragonJobConfig.APPLICATION_TOKENS_FILE);
        fsTokens.addAll(Credentials.readTokenStorageFile(jobTokenFile, conf));
        LOG.info("jobSubmitDir=" + jobSubmitDir + " jobTokenFile="
            + jobTokenFile);
        for (Token<? extends TokenIdentifier> tk : fsTokens.getAllTokens()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Token of kind " + tk.getKind()
                + "in current ugi in the AppMaster for service "
                + tk.getService());
          }
          currentUser.addToken(tk); // For use by AppMaster itself.
        }
      }
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  private class JobEventDispatcher implements EventHandler<JobEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(JobEvent event) {
      if (event.equals(JobEventType.JOB_KILL)) {
        FinishApplicationMasterRequest finishReq =
            Records.newRecord(FinishApplicationMasterRequest.class);
        finishReq.setAppAttemptId(appAttemptID);
        finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        try {
          resourceManager.finishApplicationMaster(finishReq);
        } catch (YarnRemoteException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

      ((EventHandler<JobEvent>) context.getJob(event.getJobId())).handle(event);
    }
  }

  protected DragonClientService createClientService(AppContext context) {
    return new DragonClientService(context);
  }

  private AMRMProtocol connectToRM() {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress =
        NetUtils.createSocketAddr(yarnConf.get(
            YarnConfiguration.RM_SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
  }

  private RegisterApplicationMasterResponse registerToRM()
      throws YarnRemoteException, UnknownHostException {
    InetAddress host = InetAddress.getLocalHost();

    RegisterApplicationMasterRequest appMasterRequest =
        Records.newRecord(RegisterApplicationMasterRequest.class);

    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost(host.getCanonicalHostName());
    appMasterRequest.setRpcPort(9999);
    appMasterRequest.setTrackingUrl(host.getCanonicalHostName() + ":" + 9999);

    return resourceManager.registerApplicationMaster(appMasterRequest);
  }

}
