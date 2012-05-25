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
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.DragonJobService;
import org.apache.hadoop.realtime.client.DragonClientService;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.job.allocator.ContainerAllocator;
import org.apache.hadoop.realtime.job.allocator.ContainerAllocatorEvent;
import org.apache.hadoop.realtime.job.allocator.RMContainerAllocator;
import org.apache.hadoop.realtime.job.event.ContainerLauncher;
import org.apache.hadoop.realtime.job.event.ContainerLauncherEvent;
import org.apache.hadoop.realtime.job.event.ContainerLauncherImpl;
import org.apache.hadoop.realtime.job.event.JobEvent;
import org.apache.hadoop.realtime.job.event.JobEventType;
import org.apache.hadoop.realtime.job.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.event.TaskEvent;
import org.apache.hadoop.realtime.job.event.TaskEventType;
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
  
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  
  private JobId jobId;
  private Job job;
  private JobEventDispatcher jobEventDispatcher;
  
  private TaskAttemptListener taskAttemptListener;
  
  private DragonClientService clientService;
  
  private AppContext context;
  private Dispatcher dispatcher;
  private UserGroupInformation currentUser;
  private Credentials fsTokens = new Credentials(); // Filled during init
  
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
    conf.setInt(DragonJobConfig.APPLICATION_ATTEMPT_ID, appAttemptId.getAttemptId());
     
    jobId = new JobId(appAttemptId.getApplicationId(),
        appAttemptId.getApplicationId().getId());

    // service to hand out event
    dispatcher = createDispatcher();
    addIfService(dispatcher);
    
    // service to handle requests to TaskUmbilicalProtocol
/*    taskAttemptListener = createTaskAttemptListener(context);
    addIfService(taskAttemptListener);*/

    //service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

    // Initialize the JobEventDispatcher
    this.jobEventDispatcher = new JobEventDispatcher();

    // register the event dispatchers
    dispatcher.register(JobEventType.class, jobEventDispatcher);
/*    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, new TaskAttemptEventDispatcher());
   
    // service to allocate containers from RM
    containerAllocator = createContainerAllocator(clientService, context);
    addIfService(containerAllocator);
    dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

    // corresponding service to launch allocated containers via NodeManager
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher);
    dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);*/

    super.init(conf);
  } // end of init()
  @Override
  public void start() {
    job = createJob(getConfig());
    JobEvent initJobEvent = new JobEvent(job.getJobId(), JobEventType.JOB_INIT);
    jobEventDispatcher.handle(initJobEvent);
    super.start();
  }
  
  /**
   * Initial DragonAppMaster
   * Get and CheckOut necessary parameters from system environment
   * eg: container_Id,host,port,http_port,submitTime
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
          new DragonAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime);
      Runtime.getRuntime().addShutdownHook(
          new CompositeServiceShutdownHook(appMaster));
      YarnConfiguration conf = new YarnConfiguration();
      conf.addResource(new Path(DragonJobConfig.JOB_CONF_FILE));
      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());
      conf.set(DragonJobConfig.USER_NAME, jobUserName);
      initAndStartAppMaster(appMaster, conf, jobUserName);
      
    } catch (Throwable t) {
      LOG.fatal("Error starting DragonAppMaster", t);
      System.exit(1);
    }
  }
  
  protected static void initAndStartAppMaster(final DragonAppMaster appMaster,
      final YarnConfiguration conf, String jobUserName) throws IOException,
      InterruptedException {
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
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
    JobInApplicationMaster newJob = new JobInApplicationMaster(jobId, appAttemptId, conf, dispatcher
        .getEventHandler(), taskAttemptListener,
        fsTokens, clock, currentUser.getUserName(), appSubmitTime);
    ((RunningAppContext) context).jobs.put(newJob.getJobId(), newJob);
    return newJob;
  } // end createJob()
  

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }
  private class RunningAppContext implements AppContext {

    private final Map<JobId, JobInApplicationMaster> jobs = new ConcurrentHashMap<JobId, JobInApplicationMaster>();
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
    public JobInApplicationMaster getJob(JobId jobID) {
      return jobs.get(jobID);
    }

    @Override
    public Map<JobId, JobInApplicationMaster> getAllJobs() {
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
  
  protected TaskAttemptListener createTaskAttemptListener(AppContext context) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpl(context);
    return lis;
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
      ((EventHandler<JobEvent>)context.getJob(event.getJobId())).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task = ((JobInApplicationMaster)context.getJob(event.getTaskID().getJobId())).getTask(
          event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher 
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      Job job = context.getJob(event.getTaskAttemptID().getTaskId().getJobId());
      Task task = ((JobInApplicationMaster)job).getTask(event.getTaskAttemptID().getTaskId());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }
  
  protected ContainerAllocator createContainerAllocator(
      final DragonJobService clientService, final AppContext context) {
    return new ContainerAllocatorRouter(clientService, context);
  }

  /**
   * By the time life-cycle of this router starts, job-init would have already
   * happened.
   */
  private final class ContainerAllocatorRouter extends AbstractService
      implements ContainerAllocator {
    private final DragonJobService clientService;
    private final AppContext context;
    private ContainerAllocator containerAllocator;

    ContainerAllocatorRouter(DragonJobService clientService,
        AppContext context) {
      super(ContainerAllocatorRouter.class.getName());
      this.clientService = clientService;
      this.context = context;
    }

    @Override
    public synchronized void start() {
      // TODO: uber or not?
      this.containerAllocator =
          new RMContainerAllocator(this.clientService, this.context);
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
  
  protected ContainerLauncher
  createContainerLauncher(final AppContext context) {
return new ContainerLauncherRouter(context);
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
      ((Service)this.containerLauncher).init(getConfig());
      ((Service)this.containerLauncher).start();
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
  
  protected DragonClientService createClientService(AppContext context) {
    return new DragonClientService(context);
  }

}
