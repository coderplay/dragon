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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.DragonJobGraph;
import org.apache.hadoop.realtime.DragonVertex;
import org.apache.hadoop.realtime.JobSubmissionFiles;
import org.apache.hadoop.realtime.app.counter.CountersManager;
import org.apache.hadoop.realtime.app.metrics.DragonAppMetrics;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.app.event.JobCounterUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.JobFinishEvent;
import org.apache.hadoop.realtime.job.app.event.JobTaskAttemptFetchFailureEvent;
import org.apache.hadoop.realtime.job.app.event.JobTaskEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskEvent;
import org.apache.hadoop.realtime.job.app.event.TaskEventType;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.JobTokenIdentifier;
import org.apache.hadoop.realtime.security.token.JobTokenSecretManager;
import org.apache.hadoop.realtime.serialize.HessianSerializer;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;


/** Implementation of Job interface. Maintains the state machines of Job.
 * The read and write calls use ReadWriteLock for concurrency.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class JobInAppMaster implements Job, 
  EventHandler<JobEvent> {

  private static final Log LOG = LogFactory.getLog(JobInAppMaster.class);

  //The maximum fraction of fetch failures allowed for a map
  private static final double MAX_ALLOWED_FETCH_FAILURES_FRACTION = 0.5;

  // Maximum no. of fetch-failure notifications after which map task is failed
  private static final int MAX_FETCH_FAILURES_NOTIFICATIONS = 3;
  
  //final fields
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final String username;
  private final List<AMInfo> amInfos;
  private final Lock readLock;
  private final Lock writeLock;
  private final JobId jobId;
  private final String jobName;
  private final Object tasksSyncHandle = new Object();
  private final Set<TaskId> taskIds = new LinkedHashSet<TaskId>();
  private final EventHandler eventHandler;
  private final DragonAppMetrics metrics;
  private final String userName;
  private final String queueName;
  private final long appSubmitTime;
  private final AppContext appContext;

  private final CountersManager countersInApp = new CountersManager();
  
  private DragonJobGraph jobGraph;
  private boolean lazyTasksCopyNeeded = false;
  volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  
  private final CountersManager counterManager = new CountersManager();
  private Counters jobCounters = counterManager.getCounters();
    // FIXME:  
    //
    // Can then replace task-level uber counters (MR-2424) with job-level ones
    // sent from LocalContainerLauncher, and eventually including a count of
    // of uber-AM attempts (probably sent from MRAppMaster).
  public Configuration conf;

  // fields initialized in init
  private FileSystem fs;
  private Path remoteJobSubmitDir;
  public Path remoteJobConfFile;
  private Token<JobTokenIdentifier> jobToken;
  private JobTokenSecretManager jobTokenSecretManager;
  private final List<String> diagnostics = new ArrayList<String>();
  
  //task/attempt related datastructures
  private final Map<TaskAttemptId, Integer> fetchFailuresMapping = 
    new HashMap<TaskAttemptId, Integer>();

  private static final DiagnosticsUpdateTransition
      DIAGNOSTIC_UPDATE_TRANSITION = new DiagnosticsUpdateTransition();
  private static final InternalErrorTransition
      INTERNAL_ERROR_TRANSITION = new InternalErrorTransition();
  private static final CounterUpdateTransition COUNTER_UPDATE_TRANSITION =
      new CounterUpdateTransition();

  protected static final
    StateMachineFactory<JobInAppMaster, JobState, JobEventType, JobEvent> 
       stateMachineFactory
     = new StateMachineFactory<JobInAppMaster, JobState, JobEventType, JobEvent>
              (JobState.NEW)

          // Transitions from NEW state
          .addTransition(JobState.NEW, JobState.NEW,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.NEW, JobState.NEW,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition
              (JobState.NEW,
              EnumSet.of(JobState.INITED, JobState.FAILED),
              JobEventType.JOB_INIT,
              new InitTransition())
          .addTransition(JobState.NEW, JobState.KILLED,
              JobEventType.JOB_KILL,
              new KillNewJobTransition())
          .addTransition(JobState.NEW, JobState.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from INITED state
          .addTransition(JobState.INITED, JobState.INITED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.INITED, JobState.INITED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(JobState.INITED, JobState.RUNNING,
              JobEventType.JOB_START,
              new StartTransition())
          .addTransition(JobState.INITED, JobState.KILLED,
              JobEventType.JOB_KILL,
              new KillInitedJobTransition())
          .addTransition(JobState.INITED, JobState.ERROR,
              JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from RUNNING state
          .addTransition
              (JobState.RUNNING,
              EnumSet.of(JobState.RUNNING, JobState.FAILED),
              JobEventType.JOB_TASK_COMPLETED,
              new TaskCompletedTransition())
          .addTransition
              (JobState.RUNNING,
              EnumSet.of(JobState.RUNNING, JobState.FAILED),
              JobEventType.JOB_COMPLETED,
              new JobNoTasksCompletedTransition())
          .addTransition(JobState.RUNNING, JobState.KILL_WAIT,
              JobEventType.JOB_KILL, new KillTasksTransition())
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_TASK_RESCHEDULED,
              new TaskRescheduledTransition())
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.RUNNING, JobState.RUNNING,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.RUNNING,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)

          // Transitions from KILL_WAIT state.
          .addTransition
              (JobState.KILL_WAIT,
              EnumSet.of(JobState.KILL_WAIT, JobState.KILLED),
              JobEventType.JOB_TASK_COMPLETED,
              new KillWaitTaskCompletedTransition())
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.KILL_WAIT,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.KILL_WAIT, JobState.KILL_WAIT,
              EnumSet.of(JobEventType.JOB_KILL,
                         JobEventType.JOB_TASK_RESCHEDULED))
                         
          // Transitions from FAILED state
          .addTransition(JobState.FAILED, JobState.FAILED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.FAILED, JobState.FAILED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.FAILED,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.FAILED, JobState.FAILED,
              EnumSet.of(JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_KILL))

          // Transitions from KILLED state
          .addTransition(JobState.KILLED, JobState.KILLED,
              JobEventType.JOB_DIAGNOSTIC_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(JobState.KILLED, JobState.KILLED,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          .addTransition(
              JobState.KILLED,
              JobState.ERROR, JobEventType.INTERNAL_ERROR,
              INTERNAL_ERROR_TRANSITION)
          // Ignore-able events
          .addTransition(JobState.KILLED, JobState.KILLED,
              JobEventType.JOB_KILL)

          // No transitions from INTERNAL_ERROR state. Ignore all.
          .addTransition(
              JobState.ERROR,
              JobState.ERROR,
              EnumSet.of(JobEventType.JOB_INIT,
                  JobEventType.JOB_KILL,
                  JobEventType.JOB_TASK_COMPLETED,
                  JobEventType.JOB_DIAGNOSTIC_UPDATE,
                  JobEventType.INTERNAL_ERROR))
          .addTransition(JobState.ERROR, JobState.ERROR,
              JobEventType.JOB_COUNTER_UPDATE, COUNTER_UPDATE_TRANSITION)
          // create the topology tables
          .installTopology();
 
  private final StateMachine<JobState, JobEventType, JobEvent> stateMachine;

  // changing fields while the job is running
  private int numTasks;
  private int completedTaskCount = 0;
  private boolean hasTaskFailure = false;
  private int killedTaskCount = 0;
  private long startTime;
  private long finishTime;
  private boolean isUber = false;

  private Credentials fsTokens;

  public JobInAppMaster(JobId jobId, ApplicationAttemptId applicationAttemptId,
      Configuration conf, EventHandler eventHandler,
      JobTokenSecretManager jobTokenSecretManager,
      Credentials fsTokenCredentials, Clock clock, DragonAppMetrics metrics,
      String userName, long appSubmitTime, List<AMInfo> amInfos,
      AppContext appContext) {
    this.applicationAttemptId = applicationAttemptId;
    this.jobId = jobId;
    this.jobName = conf.get(DragonJobConfig.JOB_NAME, "<missing job name>");
    this.conf = new Configuration(conf);
    this.metrics = metrics;
    this.clock = clock;
    this.amInfos = amInfos;
    this.appContext = appContext;
    this.userName = userName;
    this.queueName = conf.get(DragonJobConfig.QUEUE_NAME, "default");
    this.appSubmitTime = appSubmitTime;

    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.username = System.getProperty("user.name");
    // This "this leak" is okay because the retained pointer is in an
    // instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  EventHandler getEventHandler() {
    return this.eventHandler;
  }
  
//
//  public boolean checkAccess(UserGroupInformation callerUGI, 
//      JobACL jobOperation) {
//    AccessControlList jobACL = jobACLs.get(jobOperation);
//    if (jobACL == null) {
//      return true;
//    }
//    return aclsManager.checkAccess(callerUGI, jobOperation, username, jobACL);
//  }

  @Override
  public Counters getAllCounters() {
    readLock.lock();
    try {
      Counters counters = DragonBuilderUtils.newCounters();
      counters.addAllCounterGroups(jobCounters.getAllCounterGroups());
      return incrTaskCounters(counters, tasks.values());
    } finally {
      readLock.unlock();
    }
  }

  public static Counters incrTaskCounters(Counters counters,
      Collection<Task> tasks) {
    for (Task task : tasks) {
      counters.addAllCounterGroups(task.getCounters().getAllCounterGroups());
    }
    return counters;
  }

  @Override
  public List<String> getDiagnostics() {
    readLock.lock();
    try {
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public JobReport getReport() {
    readLock.lock();
    try {
      JobState state = getState();

      // jobFile can be null if the job is not yet inited.
      String jobFile =
          remoteJobConfFile == null ? "" : remoteJobConfFile.toString();

      return DragonBuilderUtils.newJobReport(jobId, jobName, username, state,
          appSubmitTime, startTime, finishTime, jobFile, amInfos, isUber);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Map<TaskId, Task> getTasks() {
    synchronized (tasksSyncHandle) {
      lazyTasksCopyNeeded = true;
      return Collections.unmodifiableMap(tasks);
    }
  }

  @Override
  public JobState getState() {
    readLock.lock();
    try {
     return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  protected void scheduleTasks(Set<TaskId> taskIDs) {
    for (TaskId taskID : taskIDs) {
      eventHandler.handle(new TaskEvent(taskID, 
          TaskEventType.T_SCHEDULE));
    }
  }

  @Override
  /**
   * The only entry point to change the Job.
   */
  public void handle(JobEvent event) {
    LOG.debug("Processing " + event.getJobId() + " of type " + event.getType());
    try {
      writeLock.lock();
      JobState oldState = getState();
      try {
         getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() + 
            " on Job " + this.jobId);
        eventHandler.handle(new JobEvent(this.jobId,
            JobEventType.INTERNAL_ERROR));
      }
      //notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
                 + getState());
      }
    }
    
    finally {
      writeLock.unlock();
    }
  }

  //helpful in testing
  protected void addTask(Task task) {
    synchronized (tasksSyncHandle) {
      if (lazyTasksCopyNeeded) {
        Map<TaskId, Task> newTasks = new LinkedHashMap<TaskId, Task>();
        newTasks.putAll(tasks);
        tasks = newTasks;
        lazyTasksCopyNeeded = false;
      }
    }
    tasks.put(task.getID(), task);
    taskIds.add(task.getID());
    metrics.waitingTask(task);
  }

  void setFinishTime() {
    finishTime = clock.getTime();
  }

  void logJobHistoryFinishedEvent() {
    this.setFinishTime();
//    JobFinishedEvent jfe = createJobFinishedEvent(this);
//    LOG.info("Calling handler for JobFinishedEvent ");
//    this.getEventHandler().handle(new JobHistoryEvent(this.jobId, jfe));    
  }
  
  /**
   * Create the default file System for this job.
   * 
   * @param conf the conf object
   * @return the default filesystem for this job
   * @throws IOException
   */
  protected FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }

  JobState finished(JobState finalState) {
    if (getState() == JobState.RUNNING) {
      metrics.endRunningJob(this);
    }
    if (finishTime == 0) setFinishTime();
    eventHandler.handle(new JobFinishEvent(jobId));

    switch (finalState) {
      case KILLED:
        metrics.killedJob(this);
        break;
      case FAILED:
        metrics.failedJob(this);
        break;
    }
    return finalState;
  }
  
  @Override
  public String getQueueName() {
    return queueName;
  }

  public Path getConfFile() {
    return remoteJobConfFile;
  }
  
  @Override
  public String getName() {
    return jobName;
  }

  public int getTotalTasks() {
    return tasks.size();
  }

  public List<AMInfo> getAMInfos() {
    return amInfos;
  }

  /*
  private int getBlockSize() {
    String inputClassName = conf.get(DragonJobConfig.INPUT_FORMAT_CLASS_ATTR);
    if (inputClassName != null) {
      Class<?> inputClass - Class.forName(inputClassName);
      if (FileInputFormat<K, V>)
    }
  }
  */

  public static class InitTransition 
      implements MultipleArcTransition<JobInAppMaster, JobEvent, JobState> {

    /**
     * Note that this transition method is called directly (and synchronously)
     * by MRAppMaster's init() method (i.e., no RPC, no thread-switching;
     * just plain sequential call within AM context), so we can trigger
     * modifications in AM state from here (at least, if AM is written that
     * way; MR version is).
     */
    @Override
    public JobState transition(JobInAppMaster job, JobEvent event) {
      try {
        setup(job);
        job.fs = job.getFileSystem(job.conf);

//        //log to job history
//        JobSubmittedEvent jse = new JobSubmittedEvent(job.oldJobId,
//              job.conf.get(DragonJobConfig.JOB_NAME, "test"), 
//            job.conf.get(DragonJobConfig.USER_NAME, "dragon"),
//            job.appSubmitTime,
//            job.remoteJobConfFile.toString(),
//            job.queueName);
//        job.eventHandler.handle(new JobHistoryEvent(job.jobId, jse));
        //TODO JH Verify jobACLs, UserName via UGI?

        job.jobGraph = createJobGraph(job);
        job.numTasks = getTaskCount(job.jobGraph);
        if (job.numTasks == 0) {
          job.addDiagnostic("No of tasks are 0 " + job.jobId);
        }

        checkTaskLimits();

        // create the Tasks but don't start them yet
        createTasks(job);
        job.countersInApp.getCounter(JobCounter.NUM_FAILED_TASKS).increment(54321);

        return JobState.INITED;
      } catch (IOException e) {
        LOG.warn("Job init failed", e);
        job.addDiagnostic("Job init failed : "
            + StringUtils.stringifyException(e));
        job.abortJob(JobState.FAILED);
        return job.finished(JobState.FAILED);
      }
    }

    protected void setup(JobInAppMaster job) throws IOException {
      String jodIdString = job.getID().toString();
      String user = 
        UserGroupInformation.getCurrentUser().getShortUserName();
      Path path = DragonApps.getStagingAreaDir(job.conf, user);
      if (LOG.isDebugEnabled()) {
        LOG.debug("startJobs: parent=" + path + " child=" + jodIdString);
      }

      job.remoteJobSubmitDir =
          FileSystem.get(job.conf).makeQualified(new Path(path, jodIdString));
      job.remoteJobConfFile =
          new Path(job.remoteJobSubmitDir, DragonJobConfig.JOB_CONF_FILE);

      // Prepare the TaskAttemptListener server for authentication of Containers
      // TaskAttemptListener gets the information via jobTokenSecretManager.
      JobTokenIdentifier identifier =
          new JobTokenIdentifier(new Text(jodIdString));
      job.jobToken =
          new Token<JobTokenIdentifier>(identifier, job.jobTokenSecretManager);
      job.jobToken.setService(identifier.getJobId());
      // Add it to the jobTokenSecretManager so that TaskAttemptListener server
      // can authenticate containers(tasks)
      job.jobTokenSecretManager.addTokenForJob(jodIdString, job.jobToken);
      LOG.info("Adding job token for " + jodIdString
          + " to jobTokenSecretManager");

      // Upload the jobTokens onto the remote FS so that ContainerManager can
      // localize it to be used by the Containers(tasks)
      Credentials tokenStorage = new Credentials();
      TokenCache.setJobToken(job.jobToken, tokenStorage);

      if (UserGroupInformation.isSecurityEnabled()) {
        tokenStorage.addAll(job.fsTokens);
      }
    }

    private int getTaskCount(DragonJobGraph graph) {
      int count = 0;
      for (DragonVertex vertex : graph.vertexSet()) {
        count += vertex.getTasks();
      }
      return count;
    }

    private void createTasks(JobInAppMaster job) {
      int index = 0;
      final DragonJobGraph graph = job.jobGraph;
      for (DragonVertex vertex : graph.vertexSet()) {
        for (int i = 0; i < vertex.getTasks(); i++) {
          Task task =
              new TaskInAppMaster(job.jobId, index, i, job.eventHandler,
                  job.remoteJobConfFile, job.conf, vertex, job.jobToken,
                  job.fsTokens, job.clock,
                  job.applicationAttemptId.getAttemptId(), job.metrics,
                  job.appContext);
          job.addTask(task);
        }
        index++;
      }
      LOG.info("Tasks number for job " + job.jobId + " = " + job.numTasks);
    }

    private DragonJobGraph createJobGraph(JobInAppMaster job) {
      Path descFile =
          JobSubmissionFiles.getJobDescriptionFile(job.remoteJobSubmitDir);
      try {
        FSDataInputStream in = job.fs.open(descFile);
        HessianSerializer<DragonJobGraph> serializer =
            new HessianSerializer<DragonJobGraph>();
        DragonJobGraph djg = serializer.deserialize(in);
        in.close();
        return djg;
      } catch (IOException ioe) {
        throw new YarnException(ioe);
      }
    }

    /**
     * If the number of tasks are greater than the configured value
     * throw an exception that will fail job initialization
     */
    private void checkTaskLimits() {
      // no code, for now
    }
  } // end of InitTransition

  public static class StartTransition
  implements SingleArcTransition<JobInAppMaster, JobEvent> {
    /**
     * This transition executes in the event-dispatcher thread, though it's
     * triggered in DragonAppMaster's startJobs() method.
     */
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      job.startTime = job.clock.getTime();
      job.scheduleTasks(job.taskIds);  // schedule (i.e., start) the maps
//      JobInfoChangeEvent jice = new JobInfoChangeEvent(job.oldJobId,
//          job.appSubmitTime, job.startTime);
//      job.eventHandler.handle(new JobHistoryEvent(job.jobId, jice));
      job.metrics.runningJob(job);
      
      // If we have no tasks, just transition to job completed
      if (job.numTasks == 0) {
        job.eventHandler.handle(new JobEvent(job.jobId,
            JobEventType.JOB_COMPLETED));
      }
    }
  }

  private void abortJob(JobState finalState) {
    if (finishTime == 0) setFinishTime();
//    JobUnsuccessfulCompletionEvent unsuccessfulJobEvent =
//      new JobUnsuccessfulCompletionEvent(oldJobId,
//          finishTime,
//          succeededMapTaskCount,
//          succeededReduceTaskCount,
//          finalState.toString());
//    eventHandler.handle(new JobHistoryEvent(jobId, unsuccessfulJobEvent));
  }
    
//  // JobFinishedEvent triggers the move of the history file out of the staging
//  // area. May need to create a new event type for this if JobFinished should 
//  // not be generated for KilledJobs, etc.
//  private static JobFinishedEvent createJobFinishedEvent(JobInAppMaster job) {
//
//    job.mayBeConstructFinalFullCounters();
//
//    JobFinishedEvent jfe = new JobFinishedEvent(
//        job.oldJobId, job.finishTime,
//        job.succeededMapTaskCount, job.succeededReduceTaskCount,
//        job.failedMapTaskCount, job.failedReduceTaskCount,
//        job.finalMapCounters,
//        job.finalReduceCounters,
//        job.fullCounters);
//    return jfe;
//  }

//  private void mayBeConstructFinalFullCounters() {
//    // Calculating full-counters. This should happen only once for the job.
//    synchronized (this.fullCountersLock) {
//      if (this.fullCounters != null) {
//        // Already constructed. Just return.
//        return;
//      }
//      this.constructFinalFullcounters();
//    }
//  }

//  @Private
//  public void constructFinalFullcounters() {
//    this.fullCounters = new Counters();
//    this.finalMapCounters = new Counters();
//    this.finalReduceCounters = new Counters();
//    this.fullCounters.incrAllCounters(jobCounters);
//    for (Task t : this.tasks.values()) {
//      Counters counters = t.getCounters();
//      switch (t.getType()) {
//      case MAP:
//        this.finalMapCounters.incrAllCounters(counters);
//        break;
//      case REDUCE:
//        this.finalReduceCounters.incrAllCounters(counters);
//        break;
//      }
//      this.fullCounters.incrAllCounters(counters);
//    }
//  }

  // Task-start has been moved out of InitTransition, so this arc simply
  // hardcodes 0 for both map and reduce finished tasks.
  private static class KillNewJobTransition
  implements SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      job.setFinishTime();
//      JobUnsuccessfulCompletionEvent failedEvent =
//          new JobUnsuccessfulCompletionEvent(job.oldJobId,
//              job.finishTime, 0, 0,
//              JobState.KILLED.toString());
//      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobState.KILLED);
    }
  }

  private static class KillInitedJobTransition
  implements SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      job.abortJob(JobState.KILLED);
      job.addDiagnostic("Job received Kill in INITED state.");
      job.finished(JobState.KILLED);
    }
  }

  private static class KillTasksTransition
      implements SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      job.addDiagnostic("Job received Kill while in RUNNING state.");
      for (Task task : job.tasks.values()) {
        job.eventHandler.handle(
            new TaskEvent(task.getID(), TaskEventType.T_KILL));
      }
      job.metrics.endRunningJob(job);
    }
  }

  private static class TaskAttemptFetchFailureTransition implements
      SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      JobTaskAttemptFetchFailureEvent fetchfailureEvent =
          (JobTaskAttemptFetchFailureEvent) event;
      for (TaskAttemptId attemptId : fetchfailureEvent.getMaps()) {
        Integer fetchFailures = job.fetchFailuresMapping.get(attemptId);
        fetchFailures = (fetchFailures == null) ? 1 : (fetchFailures + 1);
        job.fetchFailuresMapping.put(attemptId, fetchFailures);

        // get parallelism of respect task
        int runningTasks = 0;
        // TODO:
        // runnintTask = DragonJobGraph.getVertxt(attemptId).parallelism();

        float failureRate = (float) fetchFailures / runningTasks;
        // declare faulty if fetch-failures >= max-allowed-failures
        boolean isFaulty = (failureRate >= MAX_ALLOWED_FETCH_FAILURES_FRACTION);
        if (fetchFailures >= MAX_FETCH_FAILURES_NOTIFICATIONS && isFaulty) {
          LOG.info("Too many fetch-failures for output of task attempt: "
              + attemptId + " ... raising fetch failure to map");
          job.eventHandler.handle(new TaskAttemptEvent(attemptId,
              TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE));
          job.fetchFailuresMapping.remove(attemptId);
        }
      }
    }
  }

  private static class TaskCompletedTransition implements
      MultipleArcTransition<JobInAppMaster, JobEvent, JobState> {

    @Override
    public JobState transition(JobInAppMaster job, JobEvent event) {
      job.completedTaskCount++;
      LOG.info("Num completed Tasks: " + job.completedTaskCount);
      JobTaskEvent taskEvent = (JobTaskEvent) event;
      Task task = job.tasks.get(taskEvent.getTaskID());
      if (taskEvent.getState() == TaskState.FAILED) {
        taskFailed(job, task);
      } else if (taskEvent.getState() == TaskState.KILLED) {
        taskKilled(job, task);
      }

      return checkJobForCompletion(job);
    }

    protected JobState checkJobForCompletion(JobInAppMaster job) {
      // check for Job failure
      if (job.hasTaskFailure) {
        job.setFinishTime();

        String diagnosticMsg = "Job failed as tasks failed. " +
            "failedMaps:" + job.hasTaskFailure + 
            " failedReduces:" + job.hasTaskFailure;
        LOG.info(diagnosticMsg);
        job.addDiagnostic(diagnosticMsg);
        job.abortJob(JobState.FAILED);
        return job.finished(JobState.FAILED);
      }

      // return the current state, Job not finished yet
      return job.getState();
    }
  
    private void taskFailed(JobInAppMaster job, Task task) {
      job.hasTaskFailure = true;
      job.addDiagnostic("Task failed " + task.getID());
      job.metrics.failedTask(task);
    }

    private void taskKilled(JobInAppMaster job, Task task) {
      job.killedTaskCount++;
      job.metrics.killedTask(task);
    }
  }
 
  // Transition class for handling jobs with no tasks
  private static class JobNoTasksCompletedTransition implements
      MultipleArcTransition<JobInAppMaster, JobEvent, JobState> {

    @Override
    public JobState transition(JobInAppMaster job, JobEvent event) {
      job.setFinishTime();
      job.abortJob(JobState.FAILED);
      return job.finished(JobState.FAILED);
    }
  }

  private static class TaskRescheduledTransition implements
      SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      // succeeded map task is restarted back
      job.completedTaskCount--;
    }
  }

  private static class KillWaitTaskCompletedTransition extends
      TaskCompletedTransition {
    @Override
    protected JobState checkJobForCompletion(JobInAppMaster job) {
      if (job.completedTaskCount == job.tasks.size()) {
        job.setFinishTime();
        job.abortJob(JobState.KILLED);
        return job.finished(JobState.KILLED);
      }
      // return the current state, Job not finished yet
      return job.getState();
    }
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }
  
  private static class DiagnosticsUpdateTransition implements
      SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      job.addDiagnostic(((JobDiagnosticsUpdateEvent) event)
          .getDiagnosticUpdate());
    }
  }
  
  private static class CounterUpdateTransition implements
      SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      JobCounterUpdateEvent jce = (JobCounterUpdateEvent) event;
      for (JobCounterUpdateEvent.CounterIncrementalUpdate ci : jce
          .getCounterUpdates()) {
        job.jobCounters.getCounter(ci.getCounterKey()).increment(
          ci.getIncrementValue());
      }
    }
  }
  
  private static class InternalErrorTransition implements
      SingleArcTransition<JobInAppMaster, JobEvent> {
    @Override
    public void transition(JobInAppMaster job, JobEvent event) {
      //TODO Is this JH event required.
      job.setFinishTime();
//      JobUnsuccessfulCompletionEvent failedEvent =
//          new JobUnsuccessfulCompletionEvent(job.oldJobId,
//              job.finishTime, 0, 0,
//              JobState.ERROR.toString());
//      job.eventHandler.handle(new JobHistoryEvent(job.jobId, failedEvent));
      job.finished(JobState.ERROR);
    }
  }

  public Configuration loadConfFile() throws IOException {
    Path confPath = getConfFile();
    FileContext fc = FileContext.getFileContext(confPath.toUri(), conf);
    Configuration jobConf = new Configuration(false);
    jobConf.addResource(fc.open(confPath));
    return jobConf;
  }

  @Override
  public Credentials getCredentials() {
    return fsTokens;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public String getUser() {
    return username;
  }

  @Override
  public Map<TaskId, Task> getTasks(String label) {
    return null;
  }

  @Override
  public Task getTask(TaskId taskID) {
    return tasks.get(taskID);
  }

  @Override
  public DragonJobGraph getJobGraph() {
    return jobGraph;
  }
}
