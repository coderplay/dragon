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
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Event to record start of a task attempt
 *
 */
public class TaskAttemptStartedEvent implements HistoryEvent {
  private final TaskAttemptId attempId;
  private final long startTime;
  private final String trackerName;
  private final int httpPort;
  private final int shufflePort;
  private final ContainerId containerId;

  /**
   * Create an event to record the start of an attempt
   * @param attemptId Id of the attempt
   * @param taskIndex Index of task
   * @param startTime Start time of the attempt
   * @param trackerName Name of the Task Tracker where attempt is running
   * @param httpPort The port number of the tracker
   * @param shufflePort The shuffle port number of the container
   * @param containerId The containerId for the task attempt.
   */
  public TaskAttemptStartedEvent(
      TaskAttemptId attemptId,
       long startTime, String trackerName,
      int httpPort, int shufflePort, ContainerId containerId) {
    this.attempId = attemptId;
    this.startTime = startTime;
    this.trackerName = trackerName;
    this.httpPort = httpPort;
    this.shufflePort = shufflePort;
    this.containerId = containerId;
  }

  @Override
  public EventType getEventType() {
    return EventType.TASK_ATTEMPT_STARTED;
  }

  public TaskAttemptId getAttempId() {
    return attempId;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getTrackerName() {
    return trackerName;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public int getShufflePort() {
    return shufflePort;
  }

  public ContainerId getContainerId() {
    return containerId;
  }
}
