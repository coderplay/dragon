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

package org.apache.hadoop.realtime.job.app.event;

import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * This class encapsulates <code>ChildTask</code> related events.
 * 
 */
public class ChildExecutionEvent extends AbstractEvent<ChildExecutionEventType> {

  private ContainerId containerId;
  private ChildExecutionContext task;
  private TaskAttemptId attemptId;

  /**
   * Create a new ChildTaskEvent.
   */
  public ChildExecutionEvent(ChildExecutionEventType type, TaskAttemptId attemptId,
      ContainerId containerId, ChildExecutionContext task) {
    super(type);
    this.attemptId = attemptId;
    this.containerId = containerId;
    this.task = task;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public TaskAttemptId getTaskAttemptId() {
    return attemptId;
  }

  public ChildExecutionContext getChildTask() {
    return task;
  }
}
