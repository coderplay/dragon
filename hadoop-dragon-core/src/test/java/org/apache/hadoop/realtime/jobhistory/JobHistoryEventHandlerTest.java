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
package org.apache.hadoop.realtime.jobhistory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.jobhistory.event.JobInitedEvent;
import org.apache.hadoop.realtime.jobhistory.event.JobSubmittedEvent;
import org.apache.hadoop.realtime.jobhistory.event.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.realtime.jobhistory.event.TaskFailedEvent;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

/**
 * job history event handler tester
 */
public class JobHistoryEventHandlerTest {

  private static final Log LOG = LogFactory
      .getLog(JobHistoryEventHandlerTest.class);

  @Test
  public void testFirstFlushOnCompletionEvent() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(DragonJobConfig.JOB_HISTORY_DIR, t.workDir);
    conf.setLong(DragonJobConfig.JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l);
    conf.setInt(DragonJobConfig.JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(DragonJobConfig.JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        DragonJobConfig.JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 200);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, createJobInitedEvent()));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0; i < 100; i++) {
        queueEvent(jheh,
            new JobHistoryEvent(t.jobId, createJobSubmittedEvent()));
      }
      handleNextNEvents(jheh, 100);
      verify(mockWriter, times(0)).flush();

      // First completion event, but min-queue-size for batching flushes is 10
      handleEvent(jheh,
          new JobHistoryEvent(t.jobId, createJobCompletionEvent()));
      verify(mockWriter).flush();

    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  @Test
  public void testMaxUnflushedCompletionEvents() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(DragonJobConfig.JOB_HISTORY_DIR, t.workDir);
    conf.setLong(DragonJobConfig.JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l);
    conf.setInt(DragonJobConfig.JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(DragonJobConfig.JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        DragonJobConfig.JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 5);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, createJobInitedEvent()));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh,
            new JobHistoryEvent(t.jobId, createTaskFailureEvent()));
      }

      handleNextNEvents(jheh, 9);
      verify(mockWriter, times(0)).flush();

      handleNextNEvents(jheh, 1);
      verify(mockWriter).flush();

      handleNextNEvents(jheh, 50);
      verify(mockWriter, times(6)).flush();

    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  @Test
  public void testUnflushedTimer() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(DragonJobConfig.JOB_HISTORY_DIR, t.workDir);
    conf.setLong(DragonJobConfig.JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        2 * 1000l);
    conf.setInt(DragonJobConfig.JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 10);
    conf.setInt(DragonJobConfig.JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 100);
    conf.setInt(
        DragonJobConfig.JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 5);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, createJobInitedEvent()));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh,
            new JobHistoryEvent(t.jobId, createTaskFailureEvent()));
      }

      handleNextNEvents(jheh, 9);
      verify(mockWriter, times(0)).flush();

      Thread.sleep(2 * 4 * 1000l); // 4 seconds should be enough. Just be safe.
      verify(mockWriter).flush();
    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  @Test
  public void testBatchedFlushJobEndMultiplier() throws Exception {
    TestParams t = new TestParams();
    Configuration conf = new Configuration();
    conf.set(DragonJobConfig.JOB_HISTORY_DIR, t.workDir);
    conf.setLong(DragonJobConfig.JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
        60 * 1000l);
    conf.setInt(DragonJobConfig.JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER, 3);
    conf.setInt(DragonJobConfig.JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS, 10);
    conf.setInt(
        DragonJobConfig.JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD, 0);

    JHEvenHandlerForTest realJheh =
        new JHEvenHandlerForTest(t.mockAppContext, 0);
    JHEvenHandlerForTest jheh = spy(realJheh);
    jheh.init(conf);

    EventWriter mockWriter = null;
    try {
      jheh.start();
      handleEvent(jheh, new JobHistoryEvent(t.jobId, createJobInitedEvent()));
      mockWriter = jheh.getEventWriter();
      verify(mockWriter).write(any(HistoryEvent.class));

      for (int i = 0 ; i < 100 ; i++) {
        queueEvent(jheh,
            new JobHistoryEvent(t.jobId, createTaskFailureEvent()));
      }
      queueEvent(jheh,
          new JobHistoryEvent(t.jobId, createJobCompletionEvent()));

      handleNextNEvents(jheh, 29);
      verify(mockWriter, times(0)).flush();

      handleNextNEvents(jheh, 72);
      verify(mockWriter, times(4)).flush(); //3 * 30 + 1 for JobFinished
    } finally {
      jheh.stop();
      verify(mockWriter).close();
    }
  }

  private void queueEvent(JHEvenHandlerForTest jheh, JobHistoryEvent event) {
    jheh.handle(event);
  }

  private void handleEvent(JHEvenHandlerForTest jheh, JobHistoryEvent event)
      throws InterruptedException {
    jheh.handle(event);
    jheh.handleEvent(jheh.eventQueue.take());
  }

  private void handleNextNEvents(JHEvenHandlerForTest jheh, int numEvents)
      throws InterruptedException {
    for (int i = 0; i < numEvents; i++) {
      jheh.handleEvent(jheh.eventQueue.take());
    }
  }

  private String setupTestWorkDir() {
    File testWorkDir = new File("target", this.getClass().getCanonicalName());
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
      return testWorkDir.getAbsolutePath();
    } catch (Exception e) {
      LOG.warn("Could not cleanup", e);
      throw new YarnException("could not cleanup test dir", e);
    }
  }

  private AppContext mockAppContext(JobId jobId) {
    AppContext mockContext = mock(AppContext.class);
    Job mockJob = mock(Job.class);
    when(mockJob.getName()).thenReturn("mockjob");
    when(mockContext.getJob(jobId)).thenReturn(mockJob);
    return mockContext;
  }

  private HistoryEvent createJobCompletionEvent() {
    JobUnsuccessfulCompletionEvent jice = mock(JobUnsuccessfulCompletionEvent.class);

    when(jice.getEventType()).thenReturn(EventType.JOB_UNSUCCESSFUL_COMPLETION);

    return jice;
  }

  private HistoryEvent createJobSubmittedEvent() {
    JobSubmittedEvent jse = mock(JobSubmittedEvent.class);

    when(jse.getEventType()).thenReturn(EventType.JOB_SUBMITTED);

    return jse;
  }

  private HistoryEvent createJobInitedEvent() {
    JobInitedEvent jie = mock(JobInitedEvent.class);

    when(jie.getEventType()).thenReturn(EventType.JOB_INITED);

    return jie;
  }

  private HistoryEvent createTaskFailureEvent() {
    TaskFailedEvent tfe = mock(TaskFailedEvent.class);
    when(tfe.getEventType()).thenReturn(EventType.TASK_FAILED);
    return tfe;
  }

  private class TestParams {
    String workDir = setupTestWorkDir();
    ApplicationId appId = BuilderUtils.newApplicationId(200, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    JobId jobId = JobId.newJobId(appId, 1);
    AppContext mockAppContext = mockAppContext(jobId);
  }
}

class JHEvenHandlerForTest extends JobHistoryEventHandler {

  private EventWriter eventWriter;
  volatile int handleEventCompleteCalls = 0;
  volatile int handleEventStartedCalls = 0;

  public JHEvenHandlerForTest(AppContext context, int startCount) {
    super(context, startCount);
  }

  @Override
  public void start() {
  }

  @Override
  protected EventWriter createEventWriter(Path historyFilePath)
      throws IOException {
    this.eventWriter = mock(EventWriter.class);
    return this.eventWriter;
  }

  @Override
  protected void closeEventWriter(JobId jobId) {
  }

  public EventWriter getEventWriter() {
    return this.eventWriter;
  }

}
