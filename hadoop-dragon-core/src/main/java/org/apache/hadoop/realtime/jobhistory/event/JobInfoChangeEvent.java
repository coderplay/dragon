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
 * Event to record changes in the submit and launch time of a job
 */
public class JobInfoChangeEvent implements HistoryEvent {
  private final JobId jobId;
  private final long submitTime;
  private final long startTime;

  /**
   * Create a event to record the submit and launch time of a job
   * @param jobId Job Id
   * @param submitTime Submit time of the job
   * @param startTime Launch time of the job
   */
  public JobInfoChangeEvent(JobId jobId, long submitTime, long startTime) {
    this.jobId = jobId;
    this.submitTime = submitTime;
    this.startTime = startTime;
  }

  public JobId getJobId() {
    return jobId;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;
  }
}
