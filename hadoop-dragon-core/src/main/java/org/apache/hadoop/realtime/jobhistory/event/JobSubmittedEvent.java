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
import org.apache.hadoop.security.authorize.AccessControlList;

import java.util.Map;

/**
 * Event to record the submission of a job
 */
public class JobSubmittedEvent implements HistoryEvent {

  private final JobId jobId;
  private final String jobName;
  private final String userName;
  private final long submitTime;
  private final String jobConfPath;
  private final String jobQueueName;

  /**
   * Create an event to record job submission
   * @param jobId The job Id of the job
   * @param jobName Name of the job
   * @param userName Name of the user who submitted the job
   * @param submitTime Time of submission
   * @param jobConfPath Path of the Job Configuration file
   * @param jobQueueName The job-queue to which this job was submitted to
   */
  public JobSubmittedEvent(JobId jobId, String jobName, String userName,
                           long submitTime, String jobConfPath,
                           String jobQueueName) {
    this.jobId = jobId;
    this.jobName = jobName;
    this.userName = userName;
    this.submitTime = submitTime;
    this.jobConfPath = jobConfPath;
    this.jobQueueName = jobQueueName;
  }

  @Override
  public EventType getEventType() {
    return EventType.JOB_SUBMITTED;
  }

  public JobId getJobId() {
    return jobId;
  }

  public String getJobName() {
    return jobName;
  }

  public String getUserName() {
    return userName;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public String getJobConfPath() {
    return jobConfPath;
  }

  public String getJobQueueName() {
    return jobQueueName;
  }
}
