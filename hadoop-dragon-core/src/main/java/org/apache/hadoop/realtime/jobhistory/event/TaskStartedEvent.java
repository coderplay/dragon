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
import org.apache.hadoop.realtime.records.TaskId;

/**
 * Event to record the start of a task
 *
 */
public class TaskStartedEvent implements HistoryEvent {
  private final TaskId taskId;
  private final long startTime;
  private final String taskLabel;

  /**
   * Create an event to record start of a task
   * @param id Task Id
   * @param startTime Start time of the task
   * @param taskLabel Type of the task
   */
  public TaskStartedEvent(TaskId id, long startTime, String taskLabel) {
    this.taskId = id;
    this.startTime = startTime;
    this.taskLabel = taskLabel;
  }

  @Override
  public EventType getEventType() {
    return EventType.TASK_STARTED;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getLabel() {
    return taskLabel;
  }
}
