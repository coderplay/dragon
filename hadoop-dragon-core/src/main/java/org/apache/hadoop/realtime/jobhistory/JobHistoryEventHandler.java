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
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * class description goes here.
 *
 * @author xiaofeng_metis
 */
public class JobHistoryEventHandler extends AbstractService
    implements EventHandler<JobHistoryEvent> {

  private Thread eventHandlingThread;
  private volatile boolean stopped;
  private int eventCounter;

  private final BlockingQueue<JobHistoryEvent> eventQueue =
      new LinkedBlockingQueue<JobHistoryEvent>();

  private final Object lock = new Object();

  private final static Log LOG = LogFactory.getLog(JobHistoryEventHandler.class);

  private final static ConcurrentHashMap<JobId, MetaInfo> fileMap =
      new ConcurrentHashMap<JobId, MetaInfo>();

  private int startCount;
  private AppContext appContext;

  public JobHistoryEventHandler(AppContext context, int startCount) {
    super("");    //TODO give a name

    this.appContext = context;
    this.startCount = startCount;
  }

  @Override
  public void init(Configuration conf) {
     super.init(conf);
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
      /*if (isJobCompletionEvent(event.getHistoryEvent())) {
        // When the job is complete, flush slower but write faster.
        maxUnflushedCompletionEvents =
            maxUnflushedCompletionEvents * postJobCompletionMultiplier;
      }          */

      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  private void handleEvent(JobHistoryEvent event) {
    //To change body of created methods use File | Settings | File Templates.
  }

  private static class MetaInfo {

    public void shutDownTimer() throws IOException {
      //To change body of created methods use File | Settings | File Templates.
    }

    public void closeWriter() throws IOException {
      //To change body of created methods use File | Settings | File Templates.
    }
  }

}
