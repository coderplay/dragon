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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * class description goes here.
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {

  private Thread eventHandlingThread;
  private volatile boolean stopped;
  private int eventCounter;

  protected final BlockingQueue<JobHistoryEvent> eventQueue =
      new LinkedBlockingQueue<JobHistoryEvent>();

  private final Object lock = new Object();

  private final static Log LOG = LogFactory.getLog(JobHistoryEventHandler.class);

  private final static ConcurrentHashMap<JobId, MetaInfo> fileMap =
      new ConcurrentHashMap<JobId, MetaInfo>();

  private final int attemptId;
  private final AppContext context;

  private Path logDirPath;
  private FileSystem logDirFS;

  private int numUnflushedCompletionEvents = 0;
  private boolean isTimerActive;
  private long flushTimeout;
  private int minQueueSizeForBatchingFlushes;
  private int maxUnflushedCompletionEvents;
  private int postJobCompletionMultiplier;

  public JobHistoryEventHandler(final AppContext context, final int attemptId) {
    super("JobHistoryEventHandler");

    this.context = context;
    this.attemptId = attemptId;
  }

  @Override
  public void init(Configuration conf) {
    String logDirStr = null;
    try {
      logDirStr = JobHistoryUtils.getConfiguredHistoryStagingDirPrefix(conf);
    } catch (IOException e) {
      LOG.error("Failed while getting the configured log directories", e);
      throw new YarnException(e);
    }

    //Check for the existence of the history staging dir. Maybe create it.
    try {
      logDirPath =
          FileSystem.get(conf).makeQualified(new Path(logDirStr));
      logDirFS = FileSystem.get(logDirPath.toUri(), conf);
      mkdir(logDirFS, logDirPath, new FsPermission(
          JobHistoryUtils.HISTORY_DIR_PERMISSIONS));
    } catch (IOException e) {
      LOG.error("Failed while checking for/creating  history staging path: ["
          + logDirPath + "]", e);
      throw new YarnException(e);
    }

    // Maximum number of unflushed completion-events that can stay in the queue
    // before flush kicks in.
    maxUnflushedCompletionEvents =
        conf.getInt(DragonJobConfig.JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS,
            DragonJobConfig.DEFAULT_JOB_HISTORY_MAX_UNFLUSHED_COMPLETE_EVENTS);
    // We want to cut down flushes after job completes so as to write quicker,
    // so we increase maxUnflushedEvents post Job completion by using the
    // following multiplier.
    postJobCompletionMultiplier =
        conf.getInt(
            DragonJobConfig.JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER,
            DragonJobConfig.DEFAULT_JOB_HISTORY_JOB_COMPLETE_UNFLUSHED_MULTIPLIER);
    // Max time until which flush doesn't take place.
    flushTimeout =
        conf.getLong(DragonJobConfig.JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS,
            DragonJobConfig.DEFAULT_JOB_HISTORY_COMPLETE_EVENT_FLUSH_TIMEOUT_MS);
    minQueueSizeForBatchingFlushes =
        conf.getInt(
            DragonJobConfig.JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD,
            DragonJobConfig.DEFAULT_JOB_HISTORY_USE_BATCHED_FLUSH_QUEUE_SIZE_THRESHOLD);

    super.init(conf);
  }

  private void mkdir(FileSystem fs, Path path, FsPermission fsp) throws IOException {
    if (!fs.exists(path)) {
      try {
        fs.mkdirs(path, fsp);
        FileStatus fsStatus = fs.getFileStatus(path);
        LOG.info("Perms after creating " + fsStatus.getPermission().toShort()
            + ", Expected: " + fsp.toShort());
        if (fsStatus.getPermission().toShort() != fsp.toShort()) {
          LOG.info("Explicitly setting permissions to : " + fsp.toShort()
              + ", " + fsp);
          fs.setPermission(path, fsp);
        }
      } catch (FileAlreadyExistsException e) {
        LOG.info("Directory: [" + path + "] already exists.");
      }
    }
  }

  @Override
  public void start() {
    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        JobHistoryEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {

          // Log the size of the history-event-queue every so often.
          if (eventCounter != 0 && eventCounter % 1000 == 0) {
            eventCounter = 0;
            LOG.info("Size of the JobHistory event queue is "
                + eventQueue.size());
          } else {
            eventCounter++;
          }

          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }
          // If an event has been removed from the queue. Handle it.
          // The rest of the queue is handled via stop()
          // Clear the interrupt status if it's set before calling handleEvent
          // and set it if it was set before calling handleEvent.
          // Interrupts received from other threads during handleEvent cannot be
          // dealth with - Shell.runCommand() ignores them.
          synchronized (lock) {
            boolean isInterrupted = Thread.interrupted();
            handleEvent(event);
            if (isInterrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping JobHistoryEventHandler. "
        + "Size of the outstanding queue size is " + eventQueue.size());
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(lock) {
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }

    try {
      if (eventHandlingThread != null)
        eventHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info("Interruped Exception while stopping", ie);
    }

    // Cancel all timers - so that they aren't invoked during or after
    // the metaInfo object is wrapped up.
    for (MetaInfo mi : fileMap.values()) {
      try {
        mi.shutDownTimer();
      } catch (IOException e) {
        LOG.info("Exception while cancelling delayed flush timer. "
            + "Likely caused by a failed flush " + e.getMessage());
      }
    }

    //write all the events remaining in queue
    Iterator<JobHistoryEvent> it = eventQueue.iterator();
    while(it.hasNext()) {
      JobHistoryEvent ev = it.next();
      LOG.info("In stop, writing event " + ev.getType());
      handleEvent(ev);
    }

    //close all file handles
    for (MetaInfo mi : fileMap.values()) {
      try {
        mi.closeWriter();
      } catch (IOException e) {
        LOG.info("Exception while closing file " + e.getMessage());
      }
    }
    LOG.info("Stopped JobHistoryEventHandler. super.stop()");
    super.stop();
  }

  @Override
  public void handle(JobHistoryEvent event) {
    try {
      if (isJobCompletionEvent(event.getHistoryEvent())) {
        // When the job is complete, flush slower but write faster.
        maxUnflushedCompletionEvents =
            maxUnflushedCompletionEvents * postJobCompletionMultiplier;
      }

      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  private boolean isJobCompletionEvent(HistoryEvent historyEvent) {
    return (EnumSet.of(EventType.JOB_COMPLETED, EventType.INTERNAL_ERROR_OCCUR).
        contains(historyEvent.getEventType()));
  }

  protected void handleEvent(JobHistoryEvent event) {
    synchronized (lock) {

      // If this is JobSubmitted Event, setup the writer
      if (event.getHistoryEvent().getEventType() == EventType.JOB_INITED) {
        try {
          setupEventWriter(event.getJobId());
        } catch (IOException ioe) {
          LOG.error("Error JobHistoryEventHandler in handleEvent: " + event,
              ioe);
          throw new YarnException(ioe);
        }
      }

      // For all events
      // (1) Write it out
      // (2) Process it for JobSummary
      MetaInfo mi = fileMap.get(event.getJobId());
      try {
        HistoryEvent historyEvent = event.getHistoryEvent();
        mi.writeEvent(historyEvent);
        if (LOG.isDebugEnabled()) {
          LOG.debug("In HistoryEventHandler "
              + event.getHistoryEvent().getEventType());
        }
      } catch (IOException e) {
        LOG.error("Error writing History Event: " + event.getHistoryEvent(),
            e);
        throw new YarnException(e);
      }
    }
  }

  /**
   * Create an event writer for the Job represented by the jobID.
   * Writes out the job configuration to the log directory.
   * This should be the first call to history for a job
   *
   * @param jobId the jobId.
   * @throws IOException
   */
  protected void setupEventWriter(JobId jobId) throws IOException {

    MetaInfo oldFi = fileMap.get(jobId);

    Path historyFile = JobHistoryUtils.getJobHistoryFile(
        logDirPath, jobId, attemptId);
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    if (user == null) {
      throw new IOException(
          "User is null while setting up jobhistory eventwriter");
    }

    String jobName = context.getJob(jobId).getName();
    EventWriter writer = (oldFi == null) ? null : oldFi.writer;

    if (writer == null) {
      try {
        writer = createEventWriter(historyFile);
        LOG.info("Event Writer setup for JobId: " + jobId + ", File: "
            + historyFile);
      } catch (IOException ioe) {
        LOG.info("Could not create log file: [" + historyFile + "] + for job "
            + "[" + jobName + "]");
        throw ioe;
      }
    }

    MetaInfo fi = new MetaInfo(historyFile, writer);
    fileMap.put(jobId, fi);
  }

  @VisibleForTesting
  EventWriter createEventWriter(Path historyFile) throws IOException {
    FSDataOutputStream out = logDirFS.create(historyFile, true);
    return new EventWriter(out);
  }

  /** Close the event writer for this id
   * @throws IOException */
  public void closeWriter(JobId id) throws IOException {
    try {
      final MetaInfo mi = fileMap.get(id);
      if (mi != null) {
        mi.closeWriter();
      }

    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + id);
      throw e;
    }
  }

  @VisibleForTesting
  void closeEventWriter(JobId jobId) throws IOException {
    final MetaInfo mi = fileMap.get(jobId);
    if (mi == null) {
      throw new IOException("No MetaInfo found for JobId: [" + jobId + "]");
    }

    if (!mi.isWriterActive()) {
      throw new IOException(
          "Inactive Writer: Likely received multiple JobFinished / " +
              "JobUnsuccessful events for JobId: ["
              + jobId + "]");
    }

    // Close the Writer
    try {
      mi.closeWriter();
    } catch (IOException e) {
      LOG.error("Error closing writer for JobID: " + jobId);
      throw e;
    }

    if (mi.getHistoryFile() == null) {
      LOG.warn("No file for job-history with " + jobId + " found in cache!");
    }

  }

  private class MetaInfo {
    private Path historyFile;
    private Timer flushTimer;
    private FlushTimerTask flushTimerTask;
    private boolean isTimerShutDown = false;
    private EventWriter writer;

    public MetaInfo(Path historyFile, EventWriter writer) {
      this.historyFile = historyFile;
      this.writer = writer;

      this.flushTimer = new Timer("FlushTimer", true);
    }

    void closeWriter() throws IOException {
      synchronized (lock) {
        if (writer != null) {
          writer.close();
        }
        writer = null;
      }
    }

    void writeEvent(HistoryEvent event) throws IOException {
      synchronized (lock) {
        if (writer != null) {
          writer.write(event);
          processEventForFlush(event);
          maybeFlush(event);
        }
      }
    }

    void processEventForFlush(HistoryEvent historyEvent) throws IOException {
      if (EnumSet.of(EventType.JOB_COMPLETED).contains(historyEvent.getEventType())) {
        numUnflushedCompletionEvents++;
        if (!isTimerActive) {
          resetFlushTimer();
          if (!isTimerShutDown) {
            flushTimerTask = new FlushTimerTask(this);
            flushTimer.schedule(flushTimerTask, flushTimeout);
          }
        }
      }
    }

    void resetFlushTimer() throws IOException {
      if (flushTimerTask != null) {
        IOException exception = flushTimerTask.getException();
        flushTimerTask.stop();
        if (exception != null) {
          throw exception;
        }
        flushTimerTask = null;
      }
      isTimerActive = false;
    }

    void maybeFlush(HistoryEvent historyEvent) throws IOException {
      if ((eventQueue.size() < minQueueSizeForBatchingFlushes
          && numUnflushedCompletionEvents > 0)
          || numUnflushedCompletionEvents >= maxUnflushedCompletionEvents
          || isJobCompletionEvent(historyEvent)) {
        this.flush();
      }
    }

    void shutDownTimer() throws IOException {
      synchronized (lock) {
        isTimerShutDown = true;
        flushTimer.cancel();
        if (flushTimerTask != null && flushTimerTask.getException() != null) {
          throw flushTimerTask.getException();
        }
      }
    }

    boolean isWriterActive() {
      return writer != null;
    }

    public Path getHistoryFile() {
      return historyFile;
    }

    public boolean isTimerShutDown() {
      return isTimerShutDown;
    }

    public void flush() throws IOException {
      synchronized (lock) {
        if (numUnflushedCompletionEvents != 0) { // skipped timer cancel.
          writer.flush();
          numUnflushedCompletionEvents = 0;
          resetFlushTimer();
        }
      }
    }

  }

  private class FlushTimerTask extends TimerTask {
    private MetaInfo metaInfo;
    private IOException ioe = null;
    private volatile boolean shouldRun = true;

    FlushTimerTask(MetaInfo metaInfo) {
      this.metaInfo = metaInfo;
    }

    @Override
    public void run() {
      synchronized (lock) {
        try {
          if (!metaInfo.isTimerShutDown() && shouldRun)
            metaInfo.flush();
        } catch (IOException e) {
          ioe = e;
        }
      }
    }

    public IOException getException() {
      return ioe;
    }

    public void stop() {
      shouldRun = false;
      this.cancel();
    }
  }

}
