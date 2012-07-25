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

import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import java.text.NumberFormat;

import static org.apache.hadoop.yarn.util.StringHelper._join;

/**
 * <p>
 * <code>TaskId</code> represents the unique identifier for a Map or Reduce
 * Task.
 * </p>
 * 
 * <p>
 * TaskId consists of 3 parts. First part is <code>JobId</code>, that this Task
 * belongs to. Second part of the TaskId is either 'm' or 'r' representing
 * whether the task is a map task or a reduce task. And the third part is the
 * task number.
 * </p>
 */
public abstract class TaskId implements Comparable<TaskId> {

  /**
   * @return the associated <code>JobId</code>
   */
  public abstract JobId getJobId();


  /**
   *
   * @return
   */
  public abstract TaskType getTaskType();

  /**
   * @return the task number.
   */
  public abstract int getId();

  public abstract void setJobId(JobId jobId);

  public abstract void setTaskType(TaskType taskType);

  public abstract void setId(int id);

  public static final String TASK = "task";

  static final ThreadLocal<NumberFormat> taskIndexFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(2);
          return fmt;
        }
      };

  static final ThreadLocal<NumberFormat> taskIdFormat =
      new ThreadLocal<NumberFormat>() {
        @Override
        public NumberFormat initialValue() {
          NumberFormat fmt = NumberFormat.getInstance();
          fmt.setGroupingUsed(false);
          fmt.setMinimumIntegerDigits(6);
          return fmt;
        }
      };

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getId();
    result = prime * result + getJobId().hashCode();
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
    TaskId other = (TaskId) obj;
    if (getId() != other.getId())
      return false;
    if (!getJobId().equals(other.getJobId()))
      return false;
    return true;
  }

  @Override
  public int compareTo(TaskId other) {
    int jobIdComp = this.getJobId().compareTo(other.getJobId());
    if (jobIdComp != 0)
      return jobIdComp;
    return this.getId() - other.getId();
  }

  @Override
  public String toString() {
    return _join(TASK, getJobId().getAppId().getClusterTimestamp(),
        getJobId().getAppId().getId(), getJobId().getId(),
        getTaskType(), getId());
  }

  public static TaskId parseTaskId(String str) {
    if (str == null)
      return null;
    try {
      String[] parts = str.split(Character.toString(JobId.SEPARATOR));
      if (parts.length == 6 && parts[0].equals(TaskId.TASK)) {
        long clusterTimeStamp = Long.parseLong(parts[1]);
        int appIdInteger = Integer.parseInt(parts[2]);
        int jobIdInteger = Integer.parseInt(parts[3]);
        TaskType taskType = TaskType.valueOf(parts[4]);
        int taskIdInteger = Integer.parseInt(parts[5]);
        ApplicationId app =
            BuilderUtils.newApplicationId(clusterTimeStamp, appIdInteger);
        JobId jobId = JobId.newJobId(app, jobIdInteger);
        return newTaskId(jobId, taskIdInteger, taskType);
      }
    } catch (Exception ex) {
      // fall through
    }
    throw new IllegalArgumentException("TaskId string : " + str
        + " is not properly formed");
  }

  public static TaskId newTaskId(JobId jobId, int id,TaskType taskType) {
    TaskId taskId = Records.newRecord(TaskId.class);
    taskId.setJobId(jobId);
    taskId.setId(id);
    taskId.setTaskType(taskType);
    return taskId;
  }

}