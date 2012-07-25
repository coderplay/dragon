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

package org.apache.hadoop.realtime.records;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import static org.apache.hadoop.yarn.util.StringHelper._join;

/**
 * <p>
 * <code>TaskAttemptId</code> represents the unique identifier for a task
 * attempt. Each task attempt is one particular instance of a Map or Reduce Task
 * identified by its TaskId.
 * </p>
 * 
 * <p>
 * TaskAttemptId consists of 2 parts. First part is the <code>TaskId</code>,
 * that this <code>TaskAttemptId</code> belongs to. Second part is the task
 * attempt number.
 * </p>
 */
public abstract class TaskAttemptId implements Comparable<TaskAttemptId> {

  public static final String ATTEMPT = "attempt";

  /**
   * @return the associated TaskId.
   */
  public abstract TaskId getTaskId();

  /**
   * @return the attempt id.
   */
  public abstract int getId();

  public abstract void setTaskId(TaskId taskId);

  public abstract void setId(int id);

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result =
        prime * result + ((getTaskId() == null) ? 0 : getTaskId().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TaskAttemptId other = (TaskAttemptId) obj;
    if (getId() != other.getId())
      return false;
    if (!getTaskId().equals(other.getTaskId()))
      return false;
    return true;
  }

  @Override
  public int compareTo(TaskAttemptId other) {
    int taskIdComp = this.getTaskId().compareTo(other.getTaskId());
    if (taskIdComp == 0) {
      return this.getId() - other.getId();
    } else {
      return taskIdComp;
    }
  }

  @Override
  public String toString() {
    return _join(ATTEMPT,
        getTaskId().getJobId().getAppId().getClusterTimestamp(),
        getTaskId().getJobId().getAppId().getId(),
        getTaskId().getJobId().getId(),
        getTaskId().getTaskType(),
        getTaskId().getId(),
        getId());
  }

  public static TaskAttemptId newTaskAttemptId(TaskId taskId, int attemptId) {
    TaskAttemptId taskAttemptId = Records.newRecord(TaskAttemptId.class);
    taskAttemptId.setTaskId(taskId);
    taskAttemptId.setId(attemptId);
    return taskAttemptId;
  }

  public static TaskAttemptId parseTaskAttemptId(String str)
      throws IllegalArgumentException {
    if (str == null)
      return null;
    try {
      String[] parts = str.split(Character.toString(JobId.SEPARATOR));
      if (parts.length == 7 && parts[0].equals(ATTEMPT)) {
        long clusterTimeStamp = Long.parseLong(parts[1]);
        int appIdInteger = Integer.parseInt(parts[2]);
        int jobIdInteger = Integer.parseInt(parts[3]);
        TaskType taskType = TaskType.valueOf(parts[4]);
        int taskIdInteger = Integer.parseInt(parts[5]);
        int attemptIdInteger = Integer.parseInt(parts[6]);
        ApplicationId appId =
            BuilderUtils.newApplicationId(clusterTimeStamp, appIdInteger);
        JobId jobId = JobId.newJobId(appId, jobIdInteger);
        TaskId taskId = TaskId.newTaskId(jobId, taskIdInteger, taskType);
        return newTaskAttemptId(taskId, attemptIdInteger);
      }
    } catch (Exception ex) {
      // fall through
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str
        + " is not properly formed");
  }
}