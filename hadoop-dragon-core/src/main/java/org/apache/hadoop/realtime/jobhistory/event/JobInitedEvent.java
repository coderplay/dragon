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
package org.apache.hadoop.realtime.jobhistory.event;

import org.apache.hadoop.realtime.jobhistory.EventType;
import org.apache.hadoop.realtime.jobhistory.HistoryEvent;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Event to record the initialization of a job
 */
public class JobInitedEvent implements HistoryEvent {
  private final JobId jobId;
  private final long startTime;
  private final String jobState;


  /**
   * Create an event to record job initialization
   *
   * @param jobId
   * @param startTime
   * @param numTasks
   * @param jobState
   */
  public JobInitedEvent(JobId jobId, long startTime, String jobState) {
    this.jobId = jobId;
    this.startTime = startTime;
    this.jobState = jobState;
  }

  public JobId getJobId() {
    return jobId;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getJobState() {
    return jobState;
  }

  @Override
  public EventType getEventType() {
    return EventType.JOB_INITED;
  }

}
