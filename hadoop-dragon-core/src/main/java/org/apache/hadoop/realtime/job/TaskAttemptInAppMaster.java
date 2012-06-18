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
package org.apache.hadoop.realtime.job;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.rm.ContainerAllocator;
import org.apache.hadoop.realtime.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.realtime.app.rm.ContainerRequestEvent;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncher;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerLauncherEvent;
import org.apache.hadoop.realtime.app.rm.launcher.ContainerRemoteLaunchEvent;
import org.apache.hadoop.realtime.job.app.event.JobCounterUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptContainerLaunchedEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.realtime.job.app.event.TaskEventType;
import org.apache.hadoop.realtime.job.app.event.TaskTAttemptEvent;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.realtime.records.TaskAttemptState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.JobTokenIdentifier;
import org.apache.hadoop.realtime.server.TaskAttemptListener;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.RackResolver;

/**
 */
public class TaskAttemptInAppMaster implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {

  private static final Log LOG = LogFactory
      .getLog(TaskAttemptInAppMaster.class);

  private static final String LINE_SEPARATOR = System
      .getProperty("line.separator");

  private final TaskAttemptId attemptId;
  private final JobId jobId;
  private final Configuration conf;
  private Token<JobTokenIdentifier> jobToken;
  private Collection<Token<? extends TokenIdentifier>> fsTokens;

  private final List<String> diagnostics = new ArrayList<String>();
  protected EventHandler eventHandler;
  private final Clock clock;
  private long launchTime;
  private long finishTime;
  private final Resource resourceCapability;
  private final String[] dataLocalHosts;
  private final TaskAttemptListener taskAttemptListener;
  private final Lock readLock;
  private final Lock writeLock;
  private static Object commonContainerSpecLock = new Object();
  private static final Object classpathLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;

  private ContainerId containerID;
  private NodeId containerNodeId;
  private String containerMgrAddress;
  private String nodeHttpAddress;
  private String nodeRackName;
  private int shufflePort = -1;
  private String trackerName;
  private int httpPort;
  // this is the last status reported by the REMOTE running attempt
  private TaskAttemptStatus reportedStatus;

  private ContainerToken containerToken;
  private Resource assignedCapability;

  private TaskInChild remoteTask;

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private static final CleanupContainerTransition CLEANUP_CONTAINER_TRANSITION =
      new CleanupContainerTransition();

  private static final DiagnosticInformationUpdater DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION =
      new DiagnosticInformationUpdater();

  private static final StateMachineFactory<TaskAttemptInAppMaster, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent> stateMachineFactory =
      new StateMachineFactory<TaskAttemptInAppMaster, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>(
          TaskAttemptState.NEW)

          // Transitions from the NEW state.
          .addTransition(TaskAttemptState.NEW, TaskAttemptState.UNASSIGNED,
              TaskAttemptEventType.TA_SCHEDULE,
              new RequestContainerTransition(false))
          .addTransition(TaskAttemptState.NEW, TaskAttemptState.UNASSIGNED,
              TaskAttemptEventType.TA_RESCHEDULE,
              new RequestContainerTransition(true))
          .addTransition(TaskAttemptState.NEW, TaskAttemptState.KILLED,
              TaskAttemptEventType.TA_KILL, new KilledTransition())
          .addTransition(TaskAttemptState.NEW, TaskAttemptState.FAILED,
              TaskAttemptEventType.TA_FAILMSG, new FailedTransition())

          // Transitions from the UNASSIGNED state.
          .addTransition(TaskAttemptState.UNASSIGNED,
              TaskAttemptState.ASSIGNED, TaskAttemptEventType.TA_ASSIGNED,
              new ContainerAssignedTransition())
          .addTransition(TaskAttemptState.UNASSIGNED, TaskAttemptState.KILLED,
              TaskAttemptEventType.TA_KILL,
              new DeallocateContainerTransition(TaskAttemptState.KILLED, true))
          .addTransition(TaskAttemptState.UNASSIGNED, TaskAttemptState.FAILED,
              TaskAttemptEventType.TA_FAILMSG,
              new DeallocateContainerTransition(TaskAttemptState.FAILED, true))

          // Transitions from the ASSIGNED state.
          .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.RUNNING,
              TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
              new LaunchedContainerTransition())
          .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.ASSIGNED,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.FAILED,
              TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
              new DeallocateContainerTransition(TaskAttemptState.FAILED, false))
          .addTransition(TaskAttemptState.ASSIGNED,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_CONTAINER_COMPLETED,
              CLEANUP_CONTAINER_TRANSITION)
          // ^ If RM kills the container due to expiry, preemption etc.
          .addTransition(TaskAttemptState.ASSIGNED,
              TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)
          .addTransition(TaskAttemptState.ASSIGNED,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_FAILMSG, CLEANUP_CONTAINER_TRANSITION)

          // Transitions from RUNNING state.
          .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.RUNNING,
              TaskAttemptEventType.TA_UPDATE, new StatusUpdater())
          .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.RUNNING,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Failure handling while RUNNING
          .addTransition(TaskAttemptState.RUNNING,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_FAILMSG, CLEANUP_CONTAINER_TRANSITION)
          // for handling container exit without sending the done or fail msg
          .addTransition(TaskAttemptState.RUNNING,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_CONTAINER_COMPLETED,
              CLEANUP_CONTAINER_TRANSITION)
          // Timeout handling while RUNNING
          .addTransition(TaskAttemptState.RUNNING,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_TIMED_OUT, CLEANUP_CONTAINER_TRANSITION)
          // Kill handling
          .addTransition(TaskAttemptState.RUNNING,
              TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_KILL, CLEANUP_CONTAINER_TRANSITION)

          // Transitions from FAIL_CONTAINER_CLEANUP state.
          .addTransition(TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptState.FAIL_TASK_CLEANUP,
              TaskAttemptEventType.TA_CONTAINER_CLEANED,
              new TaskCleanupTransition())
          .addTransition(TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events
          .addTransition(
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              TaskAttemptState.FAIL_CONTAINER_CLEANUP,
              EnumSet.of(TaskAttemptEventType.TA_KILL,
                  TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                  TaskAttemptEventType.TA_UPDATE,
                  TaskAttemptEventType.TA_COMMIT_PENDING,
                  TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
                  TaskAttemptEventType.TA_DONE,
                  TaskAttemptEventType.TA_FAILMSG,
                  TaskAttemptEventType.TA_TIMED_OUT))

          // Transitions from KILL_CONTAINER_CLEANUP
          .addTransition(TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptState.KILL_TASK_CLEANUP,
              TaskAttemptEventType.TA_CONTAINER_CLEANED,
              new TaskCleanupTransition())
          .addTransition(TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events
          .addTransition(
              TaskAttemptState.KILL_CONTAINER_CLEANUP,
              TaskAttemptState.KILL_CONTAINER_CLEANUP,
              EnumSet.of(TaskAttemptEventType.TA_KILL,
                  TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                  TaskAttemptEventType.TA_UPDATE,
                  TaskAttemptEventType.TA_COMMIT_PENDING,
                  TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
                  TaskAttemptEventType.TA_DONE,
                  TaskAttemptEventType.TA_FAILMSG,
                  TaskAttemptEventType.TA_TIMED_OUT))

          // Transitions from FAIL_TASK_CLEANUP
          // run the task cleanup
          .addTransition(TaskAttemptState.FAIL_TASK_CLEANUP,
              TaskAttemptState.FAILED, TaskAttemptEventType.TA_CLEANUP_DONE,
              new FailedTransition())
          .addTransition(TaskAttemptState.FAIL_TASK_CLEANUP,
              TaskAttemptState.FAIL_TASK_CLEANUP,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events
          .addTransition(
              TaskAttemptState.FAIL_TASK_CLEANUP,
              TaskAttemptState.FAIL_TASK_CLEANUP,
              EnumSet
                  .of(TaskAttemptEventType.TA_KILL,
                      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                      TaskAttemptEventType.TA_UPDATE,
                      TaskAttemptEventType.TA_COMMIT_PENDING,
                      TaskAttemptEventType.TA_DONE,
                      TaskAttemptEventType.TA_FAILMSG))

          // Transitions from KILL_TASK_CLEANUP
          .addTransition(TaskAttemptState.KILL_TASK_CLEANUP,
              TaskAttemptState.KILLED, TaskAttemptEventType.TA_CLEANUP_DONE,
              new KilledTransition())
          .addTransition(TaskAttemptState.KILL_TASK_CLEANUP,
              TaskAttemptState.KILL_TASK_CLEANUP,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events
          .addTransition(
              TaskAttemptState.KILL_TASK_CLEANUP,
              TaskAttemptState.KILL_TASK_CLEANUP,
              EnumSet
                  .of(TaskAttemptEventType.TA_KILL,
                      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                      TaskAttemptEventType.TA_UPDATE,
                      TaskAttemptEventType.TA_COMMIT_PENDING,
                      TaskAttemptEventType.TA_DONE,
                      TaskAttemptEventType.TA_FAILMSG))

          // Transitions from FAILED state
          .addTransition(TaskAttemptState.FAILED, TaskAttemptState.FAILED,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events for FAILED state
          .addTransition(
              TaskAttemptState.FAILED,
              TaskAttemptState.FAILED,
              EnumSet
                  .of(TaskAttemptEventType.TA_KILL,
                      TaskAttemptEventType.TA_ASSIGNED,
                      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                      TaskAttemptEventType.TA_UPDATE,
                      TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
                      TaskAttemptEventType.TA_COMMIT_PENDING,
                      TaskAttemptEventType.TA_DONE,
                      TaskAttemptEventType.TA_FAILMSG))

          // Transitions from KILLED state
          .addTransition(TaskAttemptState.KILLED, TaskAttemptState.KILLED,
              TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_INFORMATION_UPDATE_TRANSITION)
          // Ignore-able events for KILLED state
          .addTransition(
              TaskAttemptState.KILLED,
              TaskAttemptState.KILLED,
              EnumSet
                  .of(TaskAttemptEventType.TA_KILL,
                      TaskAttemptEventType.TA_ASSIGNED,
                      TaskAttemptEventType.TA_CONTAINER_COMPLETED,
                      TaskAttemptEventType.TA_UPDATE,
                      TaskAttemptEventType.TA_CONTAINER_LAUNCHED,
                      TaskAttemptEventType.TA_COMMIT_PENDING,
                      TaskAttemptEventType.TA_DONE,
                      TaskAttemptEventType.TA_FAILMSG))

          // create the topology tables
          .installTopology();

  private final StateMachine<TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent> stateMachine;

  TaskAttemptInAppMaster(TaskId taskId, int i, EventHandler eventHandler,
      TaskAttemptListener taskAttemptListener, JobId jobId, Configuration conf,
      Token<JobTokenIdentifier> jobToken, String[] dataLocalHosts,
      Collection<Token<? extends TokenIdentifier>> fsTokens, Clock clock) {
    this.attemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    attemptId.setTaskId(taskId);
    attemptId.setId(i);
    this.jobId = jobId;
    this.conf = conf;
    this.jobToken = jobToken;
    this.fsTokens = fsTokens;
    this.clock = clock;
    this.eventHandler = eventHandler;
    this.taskAttemptListener = taskAttemptListener;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();

    this.resourceCapability = recordFactory.newRecordInstance(Resource.class);
    this.resourceCapability.setMemory(getMemoryRequired(conf));
    this.dataLocalHosts = dataLocalHosts;
    RackResolver.init(conf);
    stateMachine = stateMachineFactory.make(this);
  }

  private int getMemoryRequired(Configuration conf) {
    int memory = 1024;
    memory =
        conf.getInt(DragonJobConfig.TASK_MEMORY_MB,
            DragonJobConfig.DEFAULT_TASK_MEMORY_MB);

    return memory;
  }

  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }

  @Override
  public TaskAttemptReport getReport() {
    TaskAttemptReport result =
        recordFactory.newRecordInstance(TaskAttemptReport.class);
    readLock.lock();
    try {
      result.setTaskAttemptId(attemptId);
      result.setTaskAttemptState(getState());
      result.setProgress(reportedStatus.progress);
      result.setStartTime(launchTime);
      result.setFinishTime(finishTime);
      result.setDiagnosticInfo(StringUtils.join(LINE_SEPARATOR,
          getDiagnostics()));
      result.setStateString(reportedStatus.stateString);
      result.setCounters(getCounters());
      result.setContainerId(this.getAssignedContainerID());
      result.setNodeManagerHost(trackerName);
      result.setNodeManagerHttpPort(httpPort);
      if (this.containerNodeId != null) {
        result.setNodeManagerPort(this.containerNodeId.getPort());
      }
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getDiagnostics() {
    List<String> result = new ArrayList<String>();
    readLock.lock();
    try {
      result.addAll(diagnostics);
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Counters getCounters() {
    readLock.lock();
    try {
      Counters counters = reportedStatus.counters;
      if (counters == null) {
        counters = recordFactory.newRecordInstance(Counters.class);
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerId getAssignedContainerID() {
    readLock.lock();
    try {
      return containerID;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getAssignedContainerMgrAddress() {
    readLock.lock();
    try {
      return containerMgrAddress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getNodeHttpAddress() {
    readLock.lock();
    try {
      return nodeHttpAddress;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getNodeRackName() {
    this.readLock.lock();
    try {
      return this.nodeRackName;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public long getLaunchTime() {
    readLock.lock();
    try {
      return launchTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getShufflePort() {
    readLock.lock();
    try {
      return shufflePort;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  protected TaskInChild createRemoteTask() {
    return DragonBuilderUtils.newTaskInChild(attemptId.getTaskId());
  }

  private static class ContainerAssignedTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings({ "unchecked" })
    @Override
    public void transition(final TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      final TaskAttemptContainerAssignedEvent cEvent =
          (TaskAttemptContainerAssignedEvent) event;
      taskAttempt.containerID = cEvent.getContainer().getId();
      taskAttempt.containerNodeId = cEvent.getContainer().getNodeId();
      taskAttempt.containerMgrAddress = taskAttempt.containerNodeId.toString();
      taskAttempt.nodeHttpAddress = cEvent.getContainer().getNodeHttpAddress();
      taskAttempt.nodeRackName =
          RackResolver.resolve(taskAttempt.containerNodeId.getHost())
              .getNetworkLocation();
      taskAttempt.containerToken = cEvent.getContainer().getContainerToken();
      taskAttempt.assignedCapability = cEvent.getContainer().getResource();
      // TODO:this is a _real_ Task (classic Hadoop DRAGON flavor):
      taskAttempt.remoteTask = taskAttempt.createRemoteTask();
      taskAttempt.taskAttemptListener.registerPendingTask(
          taskAttempt.remoteTask, taskAttempt.containerID.toString());

      // launch the container
      // create the container object to be launched for a given Task attempt
      ContainerLaunchContext launchContext =
          createContainerLaunchContext(cEvent.getApplicationACLs(),
              taskAttempt.containerID, taskAttempt.conf, taskAttempt.jobToken,
              taskAttempt.attemptId, taskAttempt.jobId,
              taskAttempt.assignedCapability, taskAttempt.taskAttemptListener,
              taskAttempt.fsTokens);
      taskAttempt.eventHandler.handle(new ContainerRemoteLaunchEvent(
          taskAttempt.attemptId, taskAttempt.containerID,
          taskAttempt.containerMgrAddress, taskAttempt.containerToken,
          launchContext, taskAttempt.remoteTask));
    }
  }

  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      ContainerId containerId, Configuration conf,
      Token<JobTokenIdentifier> jobToken, TaskAttemptId attemptId,
      final JobId jobId, Resource assignedCapability,
      TaskAttemptListener taskAttemptListener,
      Collection<Token<? extends TokenIdentifier>> fsTokens) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec =
            createCommonContainerLaunchContext(applicationACLs, conf, jobToken,
                jobId, fsTokens);
      }
    }

    // Fill in the fields needed per-container that are missing in the common
    // spec.

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    DragonChildJVM.setVMEnv(myEnv, conf);

    // Set up the launch command
    List<String> commands =
        DragonChildJVM.getVMCommand(taskAttemptListener.getAddress(),
            attemptId, conf, containerId);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData()
        .entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container =
        BuilderUtils.newContainerLaunchContext(containerId,
            commonContainerSpec.getUser(), assignedCapability,
            commonContainerSpec.getLocalResources(), myEnv, commands,
            myServiceData,
            commonContainerSpec.getContainerTokens().duplicate(),
            applicationACLs);

    return container;
  }

  /**
   * Create the common {@link ContainerLaunchContext} for all attempts.
   * 
   * @param applicationACLs
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      Token<JobTokenIdentifier> jobToken, final JobId jobId,
      Collection<Token<? extends TokenIdentifier>> fsTokens) {

    // Application resources
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer tokens = ByteBuffer.wrap(new byte[] {});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // //////////// Set up JobJar to be localized properly on the remote NM.
      String jobJar = conf.get(DragonJobConfig.JOB_JAR);
      if (jobJar != null) {
        Path remoteJobJar =
            (new Path(jobJar)).makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());
        localResources.put(
            DragonJobConfig.JOB_JAR,
            createLocalResource(remoteFS, remoteJobJar, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION));
        LOG.info("The job-jar file on the remote FS is "
            + remoteJobJar.toUri().toASCIIString());
      } else {
        // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
        // DRAGONuce jar itself which is already on the classpath.
        LOG.info("Job jar is not present. "
            + "Not adding any jar to the list of resources.");
      }
      // //////////// End of JobJar setup

      // //////////// Set up JobConf to be localized properly on the remote NM.
      Path path =
          DragonApps.getStagingAreaDir(conf, UserGroupInformation
              .getCurrentUser().getShortUserName());
      Path remoteJobSubmitDir = new Path(path, jobId.toString());
      Path remoteJobConfPath =
          new Path(remoteJobSubmitDir, DragonJobConfig.JOB_CONF_FILE);
      localResources.put(
          DragonJobConfig.JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath,
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is "
          + remoteJobConfPath.toUri().toASCIIString());
      // //////////// End of JobConf setup

      // Setup up tokens
      Credentials taskCredentials = new Credentials();

      if (UserGroupInformation.isSecurityEnabled()) {
        // Add file-system tokens
        for (Token<? extends TokenIdentifier> token : fsTokens) {
          LOG.info("Putting fs-token for NM use for launching container : "
              + token.toString());
          taskCredentials.addToken(token.getService(), token);
        }
      }

      // LocalStorageToken is needed irrespective of whether security is enabled
      // or not.
      TokenCache.setJobToken(jobToken, taskCredentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is "
          + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      tokens =
          ByteBuffer.wrap(containerTokens_dob.getData(), 0,
              containerTokens_dob.getLength());

      // TODO:Add shuffle token
      LOG.info("Putting shuffle token in serviceData");
      /*
       * serviceData.put(ShuffleHandler.DRAGONUCE_SHUFFLE_SERVICEID,
       * ShuffleHandler.serializeServiceData(jobToken));
       */

      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          getInitialClasspath(conf));
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Shell
    environment
        .put(Environment.SHELL.name(), conf.get(
            DragonJobConfig.DRAGON_ADMIN_USER_SHELL,
            DragonJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(environment, conf.get(
        DragonJobConfig.DRAGON_ADMIN_USER_ENV,
        DragonJobConfig.DEFAULT_DRAGON_ADMIN_USER_ENV));

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container =
        BuilderUtils.newContainerLaunchContext(null,
            conf.get(DragonJobConfig.USER_NAME), null, localResources,
            environment, null, serviceData, tokens, applicationACLs);

    return container;
  }

  /**
   * Create a {@link LocalResource} record with all the given parameters.
   */
  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL =
        ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat.getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return BuilderUtils.newLocalResource(resourceURL, type, visibility,
        resourceSize, resourceModificationTime);
  }

  private static class DeallocateContainerTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    private final TaskAttemptState finalState;
    private final boolean withdrawsContainerRequest;

    DeallocateContainerTransition(TaskAttemptState finalState,
        boolean withdrawsContainerRequest) {
      this.finalState = finalState;
      this.withdrawsContainerRequest = withdrawsContainerRequest;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // set the finish time
      taskAttempt.setFinishTime();
      // send the deallocate event to ContainerAllocator
      taskAttempt.eventHandler.handle(new ContainerAllocatorEvent(
          taskAttempt.attemptId,
          ContainerAllocator.EventType.CONTAINER_DEALLOCATE));

      switch (finalState) {
      case FAILED:
        taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
            taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));
        break;
      case KILLED:
        taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
            taskAttempt.attemptId, TaskEventType.T_ATTEMPT_KILLED));
        break;
      }
    }
  }

  private static class StatusUpdater implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // Status update calls don't really change the state of the attempt.
      TaskAttemptStatus newReportedStatus =
          ((TaskAttemptStatusUpdateEvent) event).getReportedTaskAttemptStatus();
      // Now switch the information in the reportedStatus
      taskAttempt.reportedStatus = newReportedStatus;
      taskAttempt.reportedStatus.taskState = taskAttempt.getState();

      // if fetch failures are present, send the fetch failure event to job
      // this only will happen in reduce attempt type
      if (taskAttempt.reportedStatus.sendFailedTasks != null
          && taskAttempt.reportedStatus.sendFailedTasks.size() > 0) {
        taskAttempt.eventHandler.handle(new JobTaskAttemptFetchFailureEvent(
            taskAttempt.attemptId, taskAttempt.reportedStatus.sendFailedTasks));
      }
    }
  }

  static class RequestContainerTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    private final boolean rescheduled;

    public RequestContainerTransition(boolean rescheduled) {
      this.rescheduled = rescheduled;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // request for container
      if (rescheduled) {
        taskAttempt.eventHandler.handle(ContainerRequestEvent
            .createContainerRequestEventForFailedContainer(
                taskAttempt.attemptId, taskAttempt.resourceCapability));
      } else {
              Set<String> racks = new HashSet<String>();
        racks.add(NetworkTopology.DEFAULT_RACK);
        taskAttempt.eventHandler.handle(new ContainerRequestEvent(
            taskAttempt.attemptId, taskAttempt.resourceCapability, taskAttempt
                .resolveHosts(new String[] { "dw94.kgb.sqa.cm4"}), racks
                .toArray(new String[racks.size()])));}
    }
  }

  private static class LaunchedContainerTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent evnt) {

      TaskAttemptContainerLaunchedEvent event =
          (TaskAttemptContainerLaunchedEvent) evnt;

      // set the launch time
      taskAttempt.launchTime = taskAttempt.clock.getTime();
      taskAttempt.shufflePort = event.getShufflePort();

      // register it to TaskAttemptListener so that it can start monitoring it.
      taskAttempt.taskAttemptListener.registerLaunchedTask(
          taskAttempt.attemptId, taskAttempt.containerID.toString());
      // TODO Resolve to host / IP in case of a local address.
      InetSocketAddress nodeHttpInetAddr =
          NetUtils.createSocketAddr(taskAttempt.nodeHttpAddress); // TODO:
                                                                  // Costly?
      taskAttempt.trackerName = nodeHttpInetAddr.getHostName();
      taskAttempt.httpPort = nodeHttpInetAddr.getPort();
      JobCounterUpdateEvent jce =
          new JobCounterUpdateEvent(taskAttempt.attemptId.getTaskId()
              .getJobId());
      jce.addCounterUpdate(JobCounter.TOTAL_LAUNCHED_TASKS, 1);
      taskAttempt.eventHandler.handle(jce);

      LOG.info("TaskAttempt: [" + taskAttempt.attemptId
          + "] using containerId: [" + taskAttempt.containerID + " on NM: ["
          + taskAttempt.containerMgrAddress + "]");
      // make remoteTask reference as null as it is no more needed
      // and free up the memory
      taskAttempt.remoteTask = null;

      // tell the Task that attempt has started
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_LAUNCHED));
    }
  }

  private static class TaskCleanupTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // TODO: Cleanup the task
      taskAttempt.eventHandler.handle(new TaskAttemptEvent(
          taskAttempt.attemptId, TaskAttemptEventType.TA_CLEANUP_DONE));
      /*
       * TaskAttemptContext taskContext = new
       * TaskAttemptContextImpl(taskAttempt.conf, taskAttempt.attemptId);
       * taskAttempt.eventHandler.handle(new TaskCleanupEvent(
       * taskAttempt.attemptId, taskAttempt.committer, taskContext));
       */
    }
  }

  private static class FailedTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // set the finish time
      taskAttempt.setFinishTime();
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));
    }
  }

  private static class KilledTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // set the finish time
      taskAttempt.setFinishTime();
      // taskAttempt.logAttemptFinishedEvent(TaskAttemptState.KILLED); Not
      // logging Map/Reduce attempts in case of failure.
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_KILLED));
    }
  }

  private static class CleanupContainerTransition implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      // unregister it to TaskAttemptListener so that it stops listening
      // for it
      taskAttempt.taskAttemptListener.unregister(taskAttempt.attemptId,
          taskAttempt.containerID.toString());
      // send the cleanup event to containerLauncher
      taskAttempt.eventHandler.handle(new ContainerLauncherEvent(
          taskAttempt.attemptId, taskAttempt.containerID,
          taskAttempt.containerMgrAddress, taskAttempt.containerToken,
          ContainerLauncher.EventType.CONTAINER_REMOTE_CLEANUP));
    }
  }

  private static class DiagnosticInformationUpdater implements
      SingleArcTransition<TaskAttemptInAppMaster, TaskAttemptEvent> {
    @Override
    public void transition(TaskAttemptInAppMaster taskAttempt,
        TaskAttemptEvent event) {
      TaskAttemptDiagnosticsUpdateEvent diagEvent =
          (TaskAttemptDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + taskAttempt.attemptId + ": "
          + diagEvent.getDiagnosticInfo());
      taskAttempt.addDiagnosticInfo(diagEvent.getDiagnosticInfo());
    }
  }

  private void initTaskAttemptStatus(TaskAttemptStatus result) {
    result.progress = 0.0f;
    result.stateString = "NEW";
    result.taskState = TaskAttemptState.NEW;
    Counters counters = recordFactory.newRecordInstance(Counters.class);
    // counters.groups = new HashMap<String, CounterGroup>();
    result.counters = counters;
  }

  // always called in write lock
  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = clock.getTime();
    }
  }

  private void addDiagnosticInfo(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct a parent CLC and
   * use it for all the containers, so this should go away once the
   * mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf)
      throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      DragonApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

  protected String[] resolveHosts(String[] src) {
    String[] result = new String[src.length];
    for (int i = 0; i < src.length; i++) {
      if (isIP(src[i])) {
        result[i] = resolveHost(src[i]);
      } else {
        result[i] = src[i];
      }
    }
    return result;
  }

  protected String resolveHost(String src) {
    String result = src; // Fallback in case of failure.
    try {
      InetAddress addr = InetAddress.getByName(src);
      result = addr.getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to resolve address: " + src
          + ". Continuing to use the same.");
    }
    return result;
  }

  private static final Pattern ipPattern = // Pattern for matching ip
      Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");

  protected boolean isIP(String src) {
    return ipPattern.matcher(src).matches();
  }

  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptID() + " of type "
          + event.getType());
    }
    writeLock.lock();
    try {
      final TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(
            this.attemptId.getTaskId().getJobId(), "Invalid event " + event.getType() + 
            " on TaskAttempt " + this.attemptId));
        eventHandler.handle(new JobEvent(this.attemptId.getTaskId().getJobId(),
            JobEventType.INTERNAL_ERROR));
      }
      if (oldState != getState()) {
          LOG.info(attemptId + " TaskAttempt Transitioned from " 
           + oldState + " to "
           + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

}
