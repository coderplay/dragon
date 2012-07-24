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

import com.google.common.base.Splitter;
import org.apache.hadoop.realtime.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.realtime.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.realtime.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import java.util.Iterator;

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

  public static final char SEPARATOR = '_';
  public static final String TASKATTEMPT = "attempt";

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
  public String toString() {
    StringBuilder builder = new StringBuilder(TASKATTEMPT);
    TaskId taskId = getTaskId();
    builder.append(SEPARATOR).append(
        taskId.getJobId().getAppId().getClusterTimestamp());
    builder.append(SEPARATOR).append(
        JobId.jobIdFormat.get().format(
            getTaskId().getJobId().getAppId().getId()));
    builder.append(SEPARATOR).append(
        TaskId.taskIndexFormat.get().format(taskId.getIndex()));
    builder.append(SEPARATOR).append(
        TaskId.taskIdFormat.get().format(taskId.getId()));
    builder.append(SEPARATOR);
    builder.append(getId());
    return builder.toString();
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

  /** Construct a TaskAttemptID object from given string
   * @return constructed TaskAttemptID object or null if the given String is null
   * @throws IllegalArgumentException if the given string is malformed
   */
  public static TaskAttemptId forName(String str)
      throws IllegalArgumentException {
    if(str == null)
      return null;

    try {
      final Iterator<String> idIt = Splitter.on(SEPARATOR).split(str).iterator();
      idIt.next(); // ignore attempt

      final int clusterTimestamp = Integer.parseInt(idIt.next());
      final int appIdInteger = Integer.parseInt(idIt.next());
      final int taskIndexInteger = Integer.parseInt(idIt.next());
      final int taskIdInteger = Integer.parseInt(idIt.next());
      final int taskAttemptIdInteger = Integer.parseInt(idIt.next());

      final ApplicationId appId =
          BuilderUtils.newApplicationId(clusterTimestamp, appIdInteger);

      final JobId jobId = Records.newRecord(JobId.class);
      jobId.setAppId(appId);


      final TaskId taskId = Records.newRecord(TaskId.class);
      taskId.setId(taskIdInteger);
      taskId.setIndex(taskIndexInteger);
      taskId.setJobId(jobId);

      final TaskAttemptId taskAttemptId = Records.newRecord(TaskAttemptId.class);
      taskAttemptId.setId(taskAttemptIdInteger);
      taskAttemptId.setTaskId(taskId);

      return taskAttemptId;
    } catch (Exception ex) {
      // below here will throw illegal argument exception
    }

    throw new IllegalArgumentException("TaskAttemptId string : " + str
        + " is not properly formed");
  }
}