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

/**
 * Event to record Failed and Killed completion of jobs
 */
public class JobUnsuccessfulCompletionEvent implements HistoryEvent {
  private final JobId jobId;
  private final long finishTime;
  private final String jobStatus;

  /**
   * Create an event to record unsuccessful completion (killed/failed) of jobs
   * @param jobId Job ID
   * @param finishTime Finish time of the job
   * @param jobStatus Status of the job
   */
  public JobUnsuccessfulCompletionEvent(JobId jobId, long finishTime, String jobStatus) {
    this.jobId = jobId;
    this.finishTime = finishTime;
    this.jobStatus = jobStatus;
  }

  @Override
  public EventType getEventType() {
    return EventType.JOB_UNSUCCESSFUL_COMPLETION;
  }

  public JobId getJobId() {
    return jobId;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public String getStatus() {
    return jobStatus;
  }
}
