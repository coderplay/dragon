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
import org.apache.hadoop.realtime.records.TaskId;

/**
 * Event to record the failure of a task
 *
 */
public class TaskFailedEvent implements HistoryEvent {
  private final TaskId taskId;
  private final String label;
  private final String error;
  private final String status;
  private final TaskAttemptId failedDueToAttempt;

  /**
   * Create an event to record task failure
   * @param id Task ID
   * @param label label of the task
   * @param error Error String
   * @param status Status
   * @param failedDueToAttempt The attempt id due to which the task failed
   */
  public TaskFailedEvent(TaskId id, String label, String error, String status,
                         TaskAttemptId failedDueToAttempt) {
    this.taskId = id;
    this.label = label;
    this.error = error;
    this.status = status;
    this.failedDueToAttempt = failedDueToAttempt;
  }

  @Override
  public EventType getEventType() {
    return EventType.TASK_FAILED;
  }
}
