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

package org.apache.hadoop.realtime.local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;

class LocalJobRunnerMetrics implements Updater {
  private final MetricsRecord metricsRecord;

  private int numTasksLaunched = 0;
  private int numTasksCompleted = 0;
  private int numWaitingTasks = 0;
  
  public LocalJobRunnerMetrics(Configuration conf) {
    MetricsContext context = MetricsUtil.getContext("dragon");
    metricsRecord = MetricsUtil.createRecord(context, "jobtracker");
    context.registerUpdater(this);
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      metricsRecord.incrMetric("task_launched", numTasksLaunched);
      metricsRecord.incrMetric("task_completed", numTasksCompleted);
      metricsRecord.incrMetric("waiting_tasks", numWaitingTasks);

      numTasksLaunched = 0;
      numTasksCompleted = 0;
      numWaitingTasks = 0;
    }
    metricsRecord.update();
  }

  public synchronized void launchTask(TaskAttemptId taskAttemptId) {
    ++numTasksLaunched;
    decWaitingTasks(taskAttemptId.getTaskId().getJobId(), 1);
  }

  public synchronized void completeTask(TaskAttemptId taskAttemptId) {
    ++numTasksCompleted;
  }
  private synchronized void decWaitingTasks(JobId id, int task) {
    numWaitingTasks -= task;
  }

}
