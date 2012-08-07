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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.metrics.DragonAppMetrics;
import org.apache.hadoop.realtime.app.rm.ContainerAllocator;
import org.apache.hadoop.realtime.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.realtime.app.rm.RMContainerAllocator;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncher;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncherEvent;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncherImpl;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.JobInAppMaster;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.job.app.event.ChildExecutionEventType;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.JobFinishEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskEvent;
import org.apache.hadoop.realtime.job.app.event.TaskEventType;
import org.apache.hadoop.realtime.jobhistory.JobHistoryEvent;
import org.apache.hadoop.realtime.jobhistory.JobHistoryEventHandler;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.security.token.JobTokenSecretManager;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.realtime.zookeeper.DragonZooKeeperService;
import org.apache.hadoop.realtime.zookeeper.ZKEventType;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.ConverterUtils;

@SuppressWarnings("rawtypes")
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

  protected final DragonAppMetrics metrics;
  private List<AMInfo> amInfos;
  private AppContext context;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private JobId jobId;
  private Job job;
  private JobEventDispatcher jobEventDispatcher;

  private ClientService clientService;
  private ChildService childService;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;

  private Dispatcher dispatcher;
  protected UserGroupInformation currentUser;
  private Credentials fsTokens = new Credentials();
  private DragonZooKeeperService zkService;

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
    this.metrics = DragonAppMetrics.create();
    LOG.info("Created DragonAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void init(final Configuration conf) {

    // Get all needed security tokens.
    downloadTokensAndSetupUGI(conf);

    // Initialize application context,name,attemptId,jobId
    context = new RunningAppContext();
    appName = conf.get(DragonJobConfig.JOB_NAME, "<missing app name>");
    conf.setInt(DragonJobConfig.APPLICATION_ATTEMPT_ID,
        appAttemptId.getAttemptId());

    jobId =
        JobId.newJobId(appAttemptId.getApplicationId(),
            appAttemptId.getApplicationId().getId());

    // service to hand out event
    dispatcher = createDispatcher();
    addIfService(dispatcher);

    // service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

    //service to log job history events
    EventHandler<JobHistoryEvent> historyHandlerService =
        createJobHistoryHandler(context);
    dispatcher.register(org.apache.hadoop.realtime.jobhistory.EventType.class,
        historyHandlerService);

    // Initialize the JobEventDispatcher
    this.jobEventDispatcher = new JobEventDispatcher();

    // register the event dispatchers
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventDispatcher());

    // service to handle requests to TaskUmbilicalProtocol
    childService = createChildService(context);
    addIfService(childService);
    dispatcher.register(ChildExecutionEventType.class, childService);

    // service to allocate containers from RM (if non-uber) or to fake it (uber)
    containerAllocator = createContainerAllocator(clientService, context);
    addIfService(containerAllocator);
    dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);
    
    // corresponding service to launch allocated containers via NodeManager
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher);
    dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);

    // dragon zookeeper service
    zkService = new DragonZooKeeperService(context);
    addIfService(zkService);
    dispatcher.register(ZKEventType.class, zkService);

    // Add the JobHistoryEventHandler last so that it is properly stopped first.
    // This will guarantee that all history-events are flushed before AM goes
    // ahead with shutdown.
    // Note: Even though JobHistoryEventHandler is started last, if any
    // component creates a JobHistoryEvent in the meanwhile, it will be just be
    // queued inside the JobHistoryEventHandler
    addIfService(historyHandlerService);

    super.init(conf);
  }

  @Override
  public void start() {
    // / Create the AMInfo for the current AppMaster
    if (amInfos == null) {
      amInfos = new LinkedList<AMInfo>();
    }
    AMInfo amInfo =
        DragonBuilderUtils.newAMInfo(appAttemptId, startTime, containerID, nmHost,
            nmPort, nmHttpPort);
    amInfos.add(amInfo);

    job = createJob(getConfig());

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("DragonAppMaster");
    
    // create a job event for job intialization
    JobEvent initJobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    jobEventDispatcher.handle(initJobEvent);

    super.start();
    
    // All components have started, start the job.
    startJobs();
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
      YarnConfiguration conf = new YarnConfiguration(new DragonConfiguration());
      conf.addResource(new Path(DragonJobConfig.JOB_CONF_FILE));
      String jobUserName =
          System.getenv(ApplicationConstants.Environment.USER.name());
      conf.set(DragonJobConfig.USER_NAME, jobUserName);

      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);
      initAndStartAppMaster(appMaster, conf, jobUserName);
    } catch (Throwable t) {
      LOG.fatal("Error starting MRAppMaster", t);
      System.exit(1);
    }
  }

  protected static void initAndStartAppMaster(final DragonAppMaster appMaster,
      final Configuration conf, String jobUserName) throws IOException,
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
  protected Job createJob(Configuration conf) {
    // create single job
    Job newJob = new JobInAppMaster(jobId, conf, context);
    ((RunningAppContext) context).jobs.put(newJob.getID(), newJob);
    dispatcher.register(JobFinishEvent.Type.class,
        createJobFinishEventHandler());
    return newJob;
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  protected EventHandler<JobHistoryEvent> createJobHistoryHandler(AppContext context) {
    JobHistoryEventHandler eventHandler = new JobHistoryEventHandler(context,
        getStartCount());
    return eventHandler;
  }

  private int getStartCount() {
     return appAttemptId.getAttemptId();
  }

  private class RunningAppContext implements AppContext {

    private final Map<JobId, Job> jobs =
        new ConcurrentHashMap<JobId, Job>();

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
    public Job getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, Job> getAllJobs() {
      return jobs;
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public String getUser() {
      return currentUser.getUserName();
    }

    @Override
    public Clock getClock() {
      return clock;
    }

    @Override
    public InetSocketAddress getClientServiceAddress() {
      return clientService.getBindAddress();
    }

    @Override
    public int getClientServiceHttpPort() {
      return clientService.getHttpPort();
    }

    @Override
    public InetSocketAddress getChildServiceAddress() {
      return childService.getBindAddress();
    }

    @Override
    public JobTokenSecretManager getJobTokenSecretManager() {
      return jobTokenSecretManager;
    }

    @Override
    public List<AMInfo> getAmInfos() {
      return amInfos;
    }

    @Override
    public Credentials getFsTokens() {
      return fsTokens;
    }

    @Override
    public DragonAppMetrics getMetrics() {
      return metrics;
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
  
  protected ChildService createChildService(AppContext context) {
    ChildService lis = new DragonChildService(context);
    return lis;
  }

  protected ContainerAllocator createContainerAllocator(
      final ClientService clientService, final AppContext context) {
    return new ContainerAllocatorRouter(clientService, context);
  }
  

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) {
    return new ContainerLauncherRouter(context);
  }

  /**
   * create an event handler that handles the job finish event.
   * @return the job finish event handler.
   */
  protected EventHandler<JobFinishEvent> createJobFinishEventHandler() {
    return new JobFinishEventHandler();
  }
  private class JobFinishEventHandler implements EventHandler<JobFinishEvent> {
    @Override
    public void handle(JobFinishEvent event) {
      // TODO:currently just wait for some time so clients can know the
      // final states. Will be removed once RM come on.
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      try {
        // Stop all services
        // This will also send the final report to the ResourceManager
        LOG.info("Calling stop for all the services");
        stop();

        // Send job-end notification
      } catch (Throwable t) {
        LOG.warn("Graceful stop failed ", t);
      }

      // Cleanup staging directory
      // cleanupStagingDir();

      //Bring the process down by force.
      //Not needed after HADOOP-7140
      LOG.info("Exiting Dragon AppMaster..GoodBye!");
      sysexit();
    }
  }
  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }

  /**
   * This can be overridden to instantiate multiple jobs and create a 
   * workflow.
   *
   * TODO:  Rework the design to actually support this.  Currently much of the
   * job stuff has been moved to init() above to support uberization (MR-1220).
   * In a typical workflow, one presumably would want to uberize only a subset
   * of the jobs (the "small" ones), which is awkward with the current design.
   */
  @SuppressWarnings("unchecked")
  protected void startJobs() {
    /** create a job-start event to get this ball rolling */
    JobEvent startJobEvent = new JobEvent(job.getID(), JobEventType.JOB_START);
    /** send the job-start event. this triggers the job execution. */
    context.getEventHandler().handle(startJobEvent);
  }

  private class JobEventDispatcher implements EventHandler<JobEvent> {
    @Override
    public void handle(JobEvent event) {
      ((EventHandler<JobEvent>)context.getJob(event.getJobId())).handle(event);
    }
  }
  
  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task =
          context.getJob(event.getTaskID().getJobId()).getTask(
              event.getTaskID());
      ((EventHandler<TaskEvent>) task).handle(event);
    }
  }


  private class TaskAttemptEventDispatcher implements
      EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      Job job =
          context.getJob(event.getTaskAttemptID().getTaskId().getJobId());
      Task task = job.getTask(event.getTaskAttemptID().getTaskId());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }

  protected ClientService createClientService(AppContext context) {
    return new DragonClientService(context);
  }
  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerAllocatorRouter extends AbstractService
      implements ContainerAllocator {
    private final ClientService clientService;
    private final AppContext context;
    private ContainerAllocator containerAllocator;

    ContainerAllocatorRouter(ClientService clientService,
        AppContext context) {
      super(ContainerAllocatorRouter.class.getName());
      this.clientService = clientService;
      this.context = context;
    }

    @Override
    public synchronized void start() {
      this.containerAllocator =
          new RMContainerAllocator(this.context);
      ((Service) this.containerAllocator).init(getConfig());
      ((Service) this.containerAllocator).start();
      super.start();
    }

    @Override
    public synchronized void stop() {
      ((Service)this.containerAllocator).stop();
      super.stop();
    }

    @Override
    public void handle(ContainerAllocatorEvent event) {
      this.containerAllocator.handle(event);
    }
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerLauncherRouter extends AbstractService
      implements ContainerLauncher {
    private final AppContext context;
    private ContainerLauncher containerLauncher;

    ContainerLauncherRouter(AppContext context) {
      super(ContainerLauncherRouter.class.getName());
      this.context = context;
    }

    @Override
    public synchronized void start() {
      this.containerLauncher = new ContainerLauncherImpl(context);
      ((Service) this.containerLauncher).init(getConfig());
      ((Service) this.containerLauncher).start();
      super.start();
    }

    @Override
    public void handle(ContainerLauncherEvent event) {
        this.containerLauncher.handle(event);
    }

    @Override
    public synchronized void stop() {
      ((Service)this.containerLauncher).stop();
      super.stop();
    }
  }
  
  public Dispatcher getDispatcher() {
    return dispatcher;
  }
  
  public AppContext getContext(){
    return context;
  }
  
  public JobId getJobId(){
    return jobId;
  }
  
}
