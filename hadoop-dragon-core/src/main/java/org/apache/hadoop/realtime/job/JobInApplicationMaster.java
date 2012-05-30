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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.job.JobInApplicationMaster;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Records;

/**
 */
public class JobInApplicationMaster implements Job,
    EventHandler<JobEvent> {
  private static final Log LOG = LogFactory
      .getLog(JobInApplicationMaster.class);
  // final fields
  private final ApplicationAttemptId applicationAttemptId;
  private final Clock clock;
  private final String username;
  private final Lock readLock;
  private final Lock writeLock;
  private final String jobName;
  private final JobId jobId;
  private final EventHandler<JobEvent> eventHandler;
  private final String userName;
  private final String queueName;
  private final long appSubmitTime;

  private final List<String> diagnostics = new ArrayList<String>();

  volatile Map<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();
  private Credentials fsTokens;
  private Configuration conf;

  private final StateMachine<JobState, JobEventType, JobEvent> stateMachine;
  protected static final StateMachineFactory<JobInApplicationMaster, JobState, JobEventType, JobEvent> stateMachineFactory =
      new StateMachineFactory<JobInApplicationMaster, JobState, JobEventType, JobEvent>(
          JobState.NEW)
      // Transitions from NEW state
          .addTransition(JobState.NEW, JobState.INITED, JobEventType.JOB_INIT,
              new InitTransition())
          // Transitions from INITED state
          .addTransition(JobState.INITED, JobState.RUNNING,
              JobEventType.JOB_START, new StartTransition())
          // Transitions from RUNNING state
          .addTransition(JobState.RUNNING, JobState.SUCCEEDED,
              JobEventType.JOB_TASK_COMPLETED, new KillWaitTaskCompletedTransition())
          // Transitions from KILL_WAIT state.
          .addTransition(JobState.SUCCEEDED, JobState.KILLED,
              JobEventType.JOB_KILL, new KillTaskTransition())
          // create the topology tables
          .installTopology();

  public JobInApplicationMaster(JobId jobId,
      ApplicationAttemptId applicationAttemptId, Configuration conf,
      EventHandler<JobEvent> eventHandler, Credentials fsTokenCredentials,
      Clock clock, String userName, long appSubmitTime) {
    this.applicationAttemptId = applicationAttemptId;
    this.jobId = jobId;
    this.jobName = conf.get(DragonJobConfig.JOB_NAME, "<missing job name>");
    this.conf = conf;
    this.clock = clock;
    this.userName = userName;
    this.queueName = conf.get(DragonJobConfig.QUEUE_NAME, "default");
    this.appSubmitTime = appSubmitTime;

    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();

    this.fsTokens = fsTokenCredentials;
    this.username = System.getProperty("user.name");

    stateMachine = stateMachineFactory.make(this);
  }

  public static class InitTransition implements
      SingleArcTransition<JobInApplicationMaster, JobEvent> {
    @Override
    public void transition(JobInApplicationMaster job, JobEvent event) {
      // TODO: do something to start Job
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      job.getEventHandler().handle(
          new JobEvent(job.getID(), JobEventType.JOB_START));
    }
  }

  public static class StartTransition implements
      SingleArcTransition<JobInApplicationMaster, JobEvent> {
    @Override
    public void transition(JobInApplicationMaster job, JobEvent event) {
      // TODO: do something to Running Job
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      job.getEventHandler().handle(
          new JobEvent(job.getID(), JobEventType.JOB_TASK_COMPLETED));
    }
  }

  public static class KillWaitTaskCompletedTransition implements
      SingleArcTransition<JobInApplicationMaster, JobEvent> {
    @Override
    public void transition(JobInApplicationMaster job, JobEvent event) {
      // TODO: do something to Kill Job
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      job.getEventHandler().handle(
          new JobEvent(job.getID(), JobEventType.JOB_KILL));
    }
  }

  public static class KillTaskTransition implements
      SingleArcTransition<JobInApplicationMaster, JobEvent> {
    @Override
    public void transition(JobInApplicationMaster job, JobEvent event) {
      // TODO: do something to Clean the killed Job
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      LOG.info("Job " + job.getID() + "is killed");
    }
  }

  public Task getTask(TaskId taskId) {
    readLock.lock();
    try {
      return tasks.get(taskId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void handle(JobEvent event) {
    LOG.debug("Processing " + event.getJobId() + " of type " + event.getType());
    try {
      writeLock.lock();
      JobState oldState = getState();
      try {
        getStateMachine().doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        addDiagnostic("Invalid event " + event.getType() + " on Job "
            + this.jobId);
        eventHandler.handle(new JobEvent(this.jobId,
            JobEventType.INTERNAL_ERROR));
      }
      // notify the eventhandler of state change
      if (oldState != getState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
            + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public JobState getState() {
    readLock.lock();
    try {
      return getStateMachine().getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  private void addDiagnostic(String diag) {
    diagnostics.add(diag);
  }

  public Map<TaskId, Task> getTasks() {
    return tasks;
  }

  public EventHandler<JobEvent> getEventHandler() {
    return this.eventHandler;
  }

  public JobReport getReport() {
    readLock.lock();
    JobReport report = Records.newRecord(JobReport.class);
    report.setJobId(jobId);
    report.setJobName(jobName);
    report.setUser(userName);
    report.setJobState(getState());
    readLock.unlock();
    return report;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public String getName() {
    return jobName;
  }

  public Counters getAllCounters() {
    // TODO Auto-generated method stub
    return null;
  }

  public List<String> getDiagnostics() {
    // TODO Auto-generated method stub
    return null;
  }

  public float getProgress() {
    // TODO Auto-generated method stub
    return 0;
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  public Path getConfFile() {
    // TODO Auto-generated method stub
    return null;
  }

  public List<AMInfo> getAMInfos() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Credentials getCredentials() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getUser() {
    // TODO Auto-generated method stub
    return null;
  }

}
