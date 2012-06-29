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
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.DragonJobGraph;
import org.apache.hadoop.realtime.DragonVertex;
import org.apache.hadoop.realtime.app.rm.ContainerAllocator;
import org.apache.hadoop.realtime.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncher;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncherEvent;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.JobInAppMaster;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.job.TaskInAppMaster;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.JobFinishEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.realtime.records.TaskAttemptState;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.realtime.security.token.JobTokenSecretManager;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Assert;

/**
 * Mock DragonAppMaster. Doesn't start RPC servers. No threads are started
 * except of the event Dispatcher thread.
 */
public class DragonApp extends DragonAppMaster {

  private static final Log LOG = LogFactory.getLog(DragonApp.class);

  private final int taskNum;

  private File testWorkDir;
  private Path testAbsPath;

  public static String NM_HOST = "localhost";
  public static int NM_PORT = 1234;
  public static int NM_HTTP_PORT = 9999;

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  protected boolean autoComplete = false;

  static ApplicationId applicationId;

  static {
    applicationId = recordFactory.newRecordInstance(ApplicationId.class);
    applicationId.setClusterTimestamp(0);
    applicationId.setId(0);
  }

  public DragonApp(int tasks, boolean autoComplete, String testName,
      boolean cleanOnStart) {
    this(tasks, autoComplete, testName, cleanOnStart, 1);
  }

  public DragonApp(int tasks, boolean autoComplete, String testName,
      boolean cleanOnStart, int startCount) {
    this(BuilderUtils.newApplicationAttemptId(applicationId, startCount),
        BuilderUtils.newContainerId(
            BuilderUtils.newApplicationAttemptId(applicationId, startCount),
            startCount), tasks, autoComplete, testName, cleanOnStart,
        startCount);
  }

  public DragonApp(ApplicationAttemptId appAttemptId,
      ContainerId amContainerId, int tasks, boolean autoComplete,
      String testName, boolean cleanOnStart, int startCount) {
    super(appAttemptId, amContainerId, NM_HOST, NM_PORT, NM_HTTP_PORT, System
        .currentTimeMillis());
    this.testWorkDir = new File("target", testName);
    testAbsPath = new Path(testWorkDir.getAbsolutePath());
    LOG.info("PathUsed: " + testAbsPath);
    if (cleanOnStart) {
      testAbsPath = new Path(testWorkDir.getAbsolutePath());
      try {
        FileContext.getLocalFSFileContext().delete(testAbsPath, true);
      } catch (Exception e) {
        LOG.warn("COULD NOT CLEANUP: " + testAbsPath, e);
        throw new YarnException("could not cleanup test dir", e);
      }
    }
    this.autoComplete = autoComplete;
    this.taskNum = tasks;
  }

  public Job submit(Configuration conf) throws Exception {
    String user =
        conf.get(DragonJobConfig.USER_NAME, UserGroupInformation
            .getCurrentUser().getShortUserName());
    conf.set(DragonJobConfig.USER_NAME, user);
    conf.set(DragonJobConfig.DRAGON_AM_STAGING_DIR, testAbsPath.toString());
    init(conf);
    start();
    DefaultMetricsSystem.shutdown();
    Job job = getContext().getAllJobs().values().iterator().next();
    return job;
  }

  public void waitForState(TaskAttempt attempt, TaskAttemptState finalState)
      throws Exception {
    int timeoutSecs = 0;
    TaskAttemptReport report = attempt.getReport();
    while (!finalState.equals(report.getTaskAttemptState())
        && timeoutSecs++ < 20) {
      System.out.println("TaskAttempt State is : "
          + report.getTaskAttemptState() + " Waiting for state : " + finalState
          + "   progress : " + report.getProgress());
      report = attempt.getReport();
      Thread.sleep(500);
    }
    System.out
        .println("TaskAttempt State is : " + report.getTaskAttemptState());
    Assert.assertEquals("TaskAttempt state is not correct (timedout)",
        finalState, report.getTaskAttemptState());
  }

  public void waitForState(Task task, TaskState finalState) throws Exception {
    int timeoutSecs = 0;
    TaskReport report = task.getReport();
    while (!finalState.equals(report.getTaskState()) && timeoutSecs++ < 20) {
      System.out.println("Task State for " + task.getID() + " is : "
          + report.getTaskState() + " Waiting for state : " + finalState
          + "   progress : " + report.getProgress());
      report = task.getReport();
      Thread.sleep(500);
    }
    System.out.println("Task State is : " + report.getTaskState());
    Assert.assertEquals("Task state is not correct (timedout)", finalState,
        report.getTaskState());
  }

  public void waitForState(Job job, JobState finalState) throws Exception {
    int timeoutSecs = 0;
    JobReport report = job.getReport();
    while (!finalState.equals(report.getJobState()) && timeoutSecs++ < 20) {
      System.out.println("Job State is : " + report.getJobState()
          + " Waiting for state : " + finalState);
      report = job.getReport();
      Thread.sleep(500);
    }
    System.out.println("Job State is : " + report.getJobState());
    Assert.assertEquals("Job state is not correct (timedout)", finalState,
        job.getState());
  }

  public void waitForState(Service.STATE finalState) throws Exception {
    int timeoutSecs = 0;
    while (!finalState.equals(getServiceState()) && timeoutSecs++ < 20) {
      System.out.println("MRApp State is : " + getServiceState()
          + " Waiting for state : " + finalState);
      Thread.sleep(500);
    }
    System.out.println("MRApp State is : " + getServiceState());
    Assert.assertEquals("MRApp state is not correct (timedout)", finalState,
        getServiceState());
  }

  public void verifyCompleted() {
    for (Job job : getContext().getAllJobs().values()) {
      JobReport jobReport = job.getReport();
      System.out.println("Job start time :" + jobReport.getStartTime());
      System.out.println("Job finish time :" + jobReport.getFinishTime());
      Assert.assertTrue("Job start time is not less than finish time",
          jobReport.getStartTime() <= jobReport.getFinishTime());
      Assert.assertTrue("Job finish time is in future",
          jobReport.getFinishTime() <= System.currentTimeMillis());
      for (Task task : job.getTasks().values()) {
        TaskReport taskReport = task.getReport();
        System.out.println("Task start time : " + taskReport.getStartTime());
        System.out.println("Task finish time : " + taskReport.getFinishTime());
        /*
         * Assert.assertTrue("Task start time is not less than finish time",
         * taskReport.getStartTime() <= taskReport.getFinishTime());
         */
        for (TaskAttempt attempt : task.getAttempts().values()) {
          TaskAttemptReport attemptReport = attempt.getReport();
          Assert.assertTrue("Attempt start time is not less than finish time",
              attemptReport.getStartTime() <= attemptReport.getFinishTime());
        }
      }
    }
  }

  @Override
  protected Job createJob(Configuration conf) {
    Job newJob = new TestJob(getJobId(), conf);
    ((AppContext) getContext()).getAllJobs().put(newJob.getID(), newJob);
    getDispatcher().register(JobFinishEvent.Type.class,
        new EventHandler<JobFinishEvent>() {
          @Override
          public void handle(JobFinishEvent event) {
            stop();
          }
        });

    return newJob;
  }

  @Override
  protected ContainerLauncher createContainerLauncher(AppContext context) {
    return new MockContainerLauncher();
  }

  protected class MockContainerLauncher implements ContainerLauncher {

    // We are running locally so set the shuffle port to -1
    int shufflePort = -1;

    public MockContainerLauncher() {
    }

    @Override
    public void handle(ContainerLauncherEvent event) {
      switch (event.getType()) {
      case CONTAINER_REMOTE_LAUNCH:
        getContext().getEventHandler().handle(
            new TaskAttemptContainerLaunchedEvent(event.getTaskAttemptID(),
                shufflePort));

        attemptLaunched(event.getTaskAttemptID());
        break;
      case CONTAINER_REMOTE_CLEANUP:
        getContext().getEventHandler().handle(
            new TaskAttemptEvent(event.getTaskAttemptID(),
                TaskAttemptEventType.TA_CONTAINER_CLEANED));
        break;
      }
    }
  }

  protected void attemptLaunched(TaskAttemptId attemptID) {
    if (autoComplete) {
      // send the done event
      getContext().getEventHandler().handle(
          new TaskAttemptEvent(attemptID, TaskAttemptEventType.TA_TIMED_OUT));
    }
  }

  @Override
  protected ContainerAllocator createContainerAllocator(
      ClientService clientService, AppContext context) {
    return new ContainerAllocator() {
      private int containerCount;

      @Override
      public void handle(ContainerAllocatorEvent event) {
        ContainerId cId = recordFactory.newRecordInstance(ContainerId.class);
        cId.setApplicationAttemptId(getContext().getApplicationAttemptId());
        cId.setId(containerCount++);
        NodeId nodeId = BuilderUtils.newNodeId(NM_HOST, NM_PORT);
        Container container =
            BuilderUtils.newContainer(cId, nodeId,
                NM_HOST + ":" + NM_HTTP_PORT, null, null, null);
        JobId jobId =
            DragonBuilderUtils.newJobId(applicationId, applicationId.getId());
        getContext().getEventHandler().handle(
            new TaskAttemptContainerAssignedEvent(event.getAttemptID(),
                container, null));
      }
    };
  }

  @Override
  protected ClientService createClientService(AppContext context) {
    return new ClientService() {
      @Override
      public InetSocketAddress getBindAddress() {
        return NetUtils.createSocketAddr("localhost:9876");
      }

      @Override
      public int getHttpPort() {
        return -1;
      }
    };
  }

  @Override
  protected ChildService createChildService(AppContext context) {
    return new DragonChildService(context){
      @Override
      public InetSocketAddress getBindAddress() {
        return NetUtils.createSocketAddr("localhost:54321");
      }
    };
  }

  class TestJob extends JobInAppMaster {
    // override the init transition
    private TestInitTransition initTransition = new TestInitTransition(
        DragonApp.this.taskNum);
    StateMachineFactory<JobInAppMaster, JobState, JobEventType, JobEvent> localFactory =
        stateMachineFactory.addTransition(JobState.NEW,
            EnumSet.of(JobState.INITED, JobState.FAILED),
            JobEventType.JOB_INIT,
            // This is abusive.
            initTransition);

    private final StateMachine<JobState, JobEventType, JobEvent> localStateMachine;

    @Override
    protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
      return localStateMachine;
    }

    @SuppressWarnings("rawtypes")
    public TestJob(JobId jobId, Configuration conf) {
      super(jobId, conf, getContext());

      // This "this leak" is okay because the retained pointer is in an
      // instance variable.
      localStateMachine = localFactory.make(this);
    }
  }

  static class TestInitTransition extends JobInAppMaster.InitTransition {
    private int taskNum;

    TestInitTransition(int taskNum) {
      this.taskNum = taskNum;
    }

    @Override
    protected void setup(JobInAppMaster job) throws IOException {
      super.setup(job);
      job.setNumTasks(taskNum);
      job.remoteJobConfFile = new Path("test");
      createTasks(job);
    }

    @Override
    protected void createTasks(JobInAppMaster job) {
      for (int i = 0; i < taskNum; i++) {
        Task task =
            new TaskInAppMaster(job.getID(), 1, i, job.getEventHandler(),
                job.remoteJobConfFile, job.conf, null, null, null,
                job.getClock(), job.getAppAttemptId().getAttemptId(),
                job.getMetrics(), job.getAppContext());
        job.addTask(task);
      }
    }
  }

  @Override
  protected void downloadTokensAndSetupUGI(Configuration conf) {
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      new YarnException(e);
    }
  }

}
