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
import org.apache.hadoop.realtime.records.TaskAttemptId;

/**
 * Event to record unsuccessful (Killed/Failed) completion of task attempts
 *
 */
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {
  private final TaskAttemptId attemptId;
  private final int taskIndex;
  private final String status;
  private final long finishTime;
  private final String hostname;
  private final String rackName;
  private final String error;

  /**
   * Create an event to record the unsuccessful completion of attempts
   * @param id Attempt ID
   * @param taskIndex index of the task
   * @param status Status of the attempt
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the attempt executed
   * @param port rpc port for for the tracker
   * @param rackName Name of the rack where the attempt executed
   * @param error Error string
   */
  public TaskAttemptUnsuccessfulCompletionEvent(
    TaskAttemptId id, int taskIndex,
    String status, long finishTime,
    String hostname, int port,
    String rackName, String error) {
    this.attemptId = id;
    this.taskIndex = taskIndex;
    this.status = status;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.rackName = rackName;
    this.error = error;
  }

  @Override
  public EventType getEventType() {
    return EventType.TASK_ATTEMPT_COMPLETED;
  }

  public TaskAttemptId getAttemptId() {
    return attemptId;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  public String getStatus() {
    return status;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public String getHostname() {
    return hostname;
  }

  public String getRackName() {
    return rackName;
  }

  public String getError() {
    return error;
  }
}
