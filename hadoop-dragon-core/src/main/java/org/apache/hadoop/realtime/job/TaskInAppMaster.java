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

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.counter.CountersManager;
import org.apache.hadoop.realtime.app.metrics.DragonAppMetrics;
import org.apache.hadoop.realtime.app.rm.ContainerFailedEvent;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.app.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.JobTaskEvent;
import org.apache.hadoop.realtime.job.app.event.JobTaskRescheduledEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskEvent;
import org.apache.hadoop.realtime.job.app.event.TaskEventType;
import org.apache.hadoop.realtime.job.app.event.TaskTAttemptEvent;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.realtime.security.token.JobTokenIdentifier;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 * Implementation of Task interface.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TaskInAppMaster implements Task, EventHandler<TaskEvent> {

  private static final Log LOG = LogFactory.getLog(TaskInAppMaster.class);

  protected final Configuration conf;
  protected final Path jobFile;
  protected final int partition;
  protected final EventHandler eventHandler;
  private final TaskId taskId;
  private Map<TaskAttemptId, TaskAttempt> attempts;
  private final int maxAttempts;
  protected final Clock clock;
  private final Lock readLock;
  private final Lock writeLock;
  private long scheduledTime;
  protected final AppContext appContext;
  private final DragonAppMetrics metrics;
  
  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  

  protected Credentials credentials;
  protected Token<JobTokenIdentifier> jobToken;

  // By default, the next TaskAttempt number is zero. Changes during recovery
  protected int nextAttemptNumber = 0;
  private int finishedAttempts;// finish are total of failed and killed
  private int failedAttempts;

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;

  // counts the number of attempts that are either running or in a state where
  // they will come to be running when they get a Container
  private int numberUncompletedAttempts = 0;

  private static final SingleArcTransition<TaskInAppMaster, TaskEvent> ATTEMPT_KILLED_TRANSITION =
      new AttemptKilledTransition();
  private static final SingleArcTransition<TaskInAppMaster, TaskEvent> KILL_TRANSITION =
      new KillTransition();

  private static final StateMachineFactory<TaskInAppMaster, TaskState, TaskEventType, TaskEvent> stateMachineFactory =
      new StateMachineFactory<TaskInAppMaster, TaskState, TaskEventType, TaskEvent>(
          TaskState.NEW)

          // define the state machine of Task

          // Transitions from NEW state
          .addTransition(TaskState.NEW, TaskState.SCHEDULED,
              TaskEventType.T_SCHEDULE, new InitialScheduleTransition())
          .addTransition(TaskState.NEW, TaskState.KILLED, TaskEventType.T_KILL,
              new KillNewTransition())

          // Transitions from SCHEDULED state
          // when the first attempt is launched, the task state is set to
          // RUNNING
          .addTransition(TaskState.SCHEDULED, TaskState.RUNNING,
              TaskEventType.T_ATTEMPT_LAUNCHED, new LaunchTransition())
          .addTransition(TaskState.SCHEDULED, TaskState.KILL_WAIT,
              TaskEventType.T_KILL, KILL_TRANSITION)
          .addTransition(TaskState.SCHEDULED, TaskState.SCHEDULED,
              TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
          .addTransition(TaskState.SCHEDULED,
              EnumSet.of(TaskState.SCHEDULED, TaskState.FAILED),
              TaskEventType.T_ATTEMPT_FAILED, new AttemptFailedTransition())

          // Transitions from RUNNING state
          .addTransition(TaskState.RUNNING, TaskState.RUNNING,
              TaskEventType.T_ATTEMPT_LAUNCHED)
          .addTransition(TaskState.RUNNING, TaskState.RUNNING,
              TaskEventType.T_ATTEMPT_KILLED, ATTEMPT_KILLED_TRANSITION)
          .addTransition(TaskState.RUNNING,
              EnumSet.of(TaskState.RUNNING, TaskState.FAILED),
              TaskEventType.T_ATTEMPT_FAILED, new AttemptFailedTransition())
          .addTransition(TaskState.RUNNING, TaskState.KILL_WAIT,
              TaskEventType.T_KILL, KILL_TRANSITION)

          // Transitions from KILL_WAIT state
          .addTransition(TaskState.KILL_WAIT,
              EnumSet.of(TaskState.KILL_WAIT, TaskState.KILLED),
              TaskEventType.T_ATTEMPT_KILLED,
              new KillWaitAttemptKilledTransition())
          // Ignore-able transitions.
          .addTransition(
              TaskState.KILL_WAIT,
              TaskState.KILL_WAIT,
              EnumSet.of(TaskEventType.T_KILL,
                  TaskEventType.T_ATTEMPT_LAUNCHED,
                  TaskEventType.T_ATTEMPT_FAILED))

          // Transitions from SUCCEEDED state

          // Transitions from FAILED state
          .addTransition(TaskState.FAILED, TaskState.FAILED,
              TaskEventType.T_KILL)

          // Transitions from KILLED state
          .addTransition(TaskState.KILLED, TaskState.KILLED,
              TaskEventType.T_KILL)

          // create the topology tables
          .installTopology();

  public TaskInAppMaster(JobId jobId, int taskIndex, int partition,
      EventHandler eventHandler, Path remoteJobConfFile, Configuration conf,
      Token<JobTokenIdentifier> jobToken, Credentials credentials, Clock clock,
      int startCount, DragonAppMetrics metrics, AppContext appContext) {
    this.conf = conf;
    this.clock = clock;
    this.jobFile = remoteJobConfFile;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    this.attempts = Collections.emptyMap();
    // This overridable method call is okay in a constructor because we
    // have a convention that none of the overrides depends on any
    // fields that need initialization.
    maxAttempts = getMaxAttempts();
    taskId = DragonBuilderUtils.newTaskId(jobId, taskIndex, partition);
    this.partition = partition;
    this.eventHandler = eventHandler;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.metrics = metrics;
    this.appContext = appContext;

    // All the new TaskAttemptIDs are generated based on MR
    // ApplicationAttemptID so that attempts from previous lives don't
    // over-step the current one. This assumes that a task won't have more
    // than 1000 attempts in its single generation, which is very reasonable.
    // Someone is nuts if he/she thinks he/she can live with 1000 TaskAttempts
    // and requires serious medical attention.
    nextAttemptNumber = (startCount - 1) * 1000;

    // This "this leak" is okay because the retained pointer is in an
    // instance variable.
    stateMachine = stateMachineFactory.make(this);
  }

  @Override
  public TaskId getID() {
    return taskId;
  }

  @Override
  public TaskReport getReport() {
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    readLock.lock();
    try {
      report.setTaskId(taskId);
      report.setStartTime(getLaunchTime());
      report.setTaskState(getState());
      for (TaskAttempt attempt : attempts.values()) {
        if (TaskAttemptState.RUNNING.equals(attempt.getState())) {
          report.addRunningAttempt(attempt.getID());
        }
      }

      for (TaskAttempt att : attempts.values()) {
        String prefix = "AttemptID:" + att.getID() + " Info:";
        for (CharSequence cs : att.getDiagnostics()) {
          report.addDiagnostics(prefix + cs);
        }
      }
      // Add a copy of counters as the last step so that their lifetime on heap
      // is as small as possible.
      report.setCounters(getCounters());

      return report;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskState getState() {
    return stateMachine.getCurrentState();
  }

  @Override
  public Counters getCounters() {
    Counters counters = null;
    readLock.lock();
    try {
      TaskAttempt bestAttempt = selectBestAttempt();
      if (bestAttempt != null) {
        counters = bestAttempt.getCounters();
      } else {
        counters = DragonBuilderUtils.newCounters();
      }
      return counters;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getLabel() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    readLock.lock();
    try {
      if (attempts.size() <= 1) {
        return attempts;
      }
      Map<TaskAttemptId, TaskAttempt> result =
          new LinkedHashMap<TaskAttemptId, TaskAttempt>();
      result.putAll(attempts);
      return result;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    readLock.lock();
    try {
      return attempts.get(attemptID);
    } finally {
      readLock.unlock();
    }
  }

  private static class InitialScheduleTransition implements
      SingleArcTransition<TaskInAppMaster, TaskEvent> {

    @Override
    public void transition(TaskInAppMaster task, TaskEvent event) {
      task.addAndScheduleAttempt();
      task.scheduledTime = task.clock.getTime();
    }
  }

  // TODO: createAttempt
  protected TaskAttempt createAttempt() {
    return new TaskAttemptInAppMaster(taskId, nextAttemptNumber, eventHandler,
        jobFile, partition, conf, jobToken, credentials, clock, appContext);
  }

  // This is always called in the Write Lock
  private void addAndScheduleAttempt() {
    TaskAttempt attempt = createAttempt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created attempt " + attempt.getID());
    }
    switch (attempts.size()) {
    case 0:
      attempts = Collections.singletonMap(attempt.getID(), attempt);
      break;

    case 1:
      Map<TaskAttemptId, TaskAttempt> newAttempts =
          new LinkedHashMap<TaskAttemptId, TaskAttempt>(maxAttempts);
      newAttempts.putAll(attempts);
      attempts = newAttempts;
      attempts.put(attempt.getID(), attempt);
      break;

    default:
      attempts.put(attempt.getID(), attempt);
      break;
    }

    // Update nextATtemptNumber
    ++nextAttemptNumber;
    ++numberUncompletedAttempts;
    // schedule the nextAttemptNumber
    if (failedAttempts > 0) {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_RESCHEDULE));
    } else {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_SCHEDULE));
    }
  }

  private void killAllAttempt(TaskAttempt attempt, String logMsg) {
    if (attempt != null) {
      eventHandler.handle(new TaskAttemptEvent(attempt.getID(),
          TaskAttemptEventType.TA_KILL));
    }
  }

  private static class KillTransition implements
      SingleArcTransition<TaskInAppMaster, TaskEvent> {
    @Override
    public void transition(TaskInAppMaster task, TaskEvent event) {
      // issue kill to all non finished attempts
      for (TaskAttempt attempt : task.attempts.values()) {
        task.killAllAttempt(attempt, "Task KILL is received. Killing attempt!");
      }
      task.numberUncompletedAttempts = 0;
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<TaskInAppMaster, TaskEvent> {
    @Override
    public void transition(TaskInAppMaster task, TaskEvent event) {
      --task.numberUncompletedAttempts;
      task.addAndScheduleAttempt();
    }
  }

  static class LaunchTransition implements
      SingleArcTransition<TaskInAppMaster, TaskEvent> {
    @Override
    public void transition(TaskInAppMaster task, TaskEvent event) {
      task.metrics.launchedTask(task);
      task.metrics.runningTask(task);
    }
  }

  private static class KillNewTransition implements
      SingleArcTransition<TaskInAppMaster, TaskEvent> {
    @Override
    public void transition(TaskInAppMaster task, TaskEvent event) {

      task.eventHandler.handle(new JobTaskEvent(task.taskId, TaskState.KILLED));
      task.metrics.endWaitingTask(task);
    }
  }

  private static class AttemptFailedTransition implements
      MultipleArcTransition<TaskInAppMaster, TaskEvent, TaskState> {

    @Override
    public TaskState transition(TaskInAppMaster task, TaskEvent event) {
      task.failedAttempts++;
      TaskTAttemptEvent castEvent = (TaskTAttemptEvent) event;
      TaskAttempt attempt = task.attempts.get(castEvent.getTaskAttemptID());
      if (attempt.getAssignedContainerMgrAddress() != null) {
        // container was assigned
        task.eventHandler.handle(new ContainerFailedEvent(attempt.getID(),
            attempt.getAssignedContainerMgrAddress()));
      }

      if (task.failedAttempts < task.maxAttempts) {
        task.eventHandler
          .handle(new JobTaskRescheduledEvent(task.taskId));
        // we don't need a new event if we already have a spare
        if (--task.numberUncompletedAttempts == 0) {
          task.addAndScheduleAttempt();
        }
      } else {
        TaskTAttemptEvent ev = (TaskTAttemptEvent) event;
        TaskAttemptId taId = ev.getTaskAttemptID();

        task.eventHandler
            .handle(new JobTaskEvent(task.taskId, TaskState.FAILED));
        return task.finished(TaskState.FAILED);
      }
      return getDefaultState(task);
    }

    protected TaskState getDefaultState(Task task) {
      return task.getState();
    }

    protected void unSucceed(TaskInAppMaster task) {
      ++task.numberUncompletedAttempts;
    }
  }

  private TaskState finished(TaskState finalState) {
    if (getState() == TaskState.RUNNING) {
      metrics.endRunningTask(this);
    }
    return finalState;
  }

  private static class KillWaitAttemptKilledTransition implements
      MultipleArcTransition<TaskInAppMaster, TaskEvent, TaskState> {

    protected TaskState finalState = TaskState.KILLED;

    @Override
    public TaskState transition(TaskInAppMaster task, TaskEvent event) {
      // check whether all attempts are finished
      if (task.finishedAttempts == task.attempts.size()) {
        task.eventHandler.handle(new JobTaskEvent(task.taskId, finalState));
        return finalState;
      }
      return task.getState();
    }
  }

  // TODO:
  protected int getMaxAttempts() {
    return conf.getInt(DragonJobConfig.DRAGON_TASK_MAX_ATTEMPT,
        DragonJobConfig.DEFAULT_DRAGON_TASK_MAX_ATTEMPT);
  }

  // this is always called in read/write lock
  private long getLaunchTime() {
    long taskLaunchTime = 0;
    boolean launchTimeSet = false;
    for (TaskAttempt at : attempts.values()) {
      // select the least launch time of all attempts
      long attemptLaunchTime = at.getLaunchTime();
      if (attemptLaunchTime != 0 && !launchTimeSet) {
        // For the first non-zero launch time
        launchTimeSet = true;
        taskLaunchTime = attemptLaunchTime;
      } else if (attemptLaunchTime != 0 && taskLaunchTime > attemptLaunchTime) {
        taskLaunchTime = attemptLaunchTime;
      }
    }
    if (!launchTimeSet) {
      return this.scheduledTime;
    }
    return taskLaunchTime;
  }

  // select the nextAttemptNumber with best progress
  // always called inside the Read Lock
  private TaskAttempt selectBestAttempt() {
    TaskAttempt result = null;
    for (TaskAttempt at : attempts.values()) {
      switch (at.getState()) {

      // ignore all failed task attempts
      case FAIL_CONTAINER_CLEANUP:
      case FAIL_TASK_CLEANUP:
      case FAILED:
      case KILL_CONTAINER_CLEANUP:
      case KILL_TASK_CLEANUP:
      case KILLED:
        continue;
      }
      if (result == null) {
        result = at; 
        break;
      }
    }
    return result;
  }

  @Override
  public void handle(TaskEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskID() + " of type "
          + event.getType());
    }
    try {
      writeLock.lock();
      TaskState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error(
            "Can't handle this event at current state for " + this.taskId, e);
        internalError(event.getType());
      }
      if (oldState != getState()) {
        LOG.info(taskId + " Task Transitioned from " + oldState + " to "
            + getState());
      }

    } finally {
      writeLock.unlock();
    }
  }

  private void internalError(TaskEventType type) {
    LOG.error("Invalid event " + type + " on Task " + this.taskId);
    eventHandler.handle(new JobDiagnosticsUpdateEvent(this.taskId.getJobId(),
        "Invalid event " + type + " on Task " + this.taskId));
    eventHandler.handle(new JobEvent(this.taskId.getJobId(),
        JobEventType.INTERNAL_ERROR));
  }
}
