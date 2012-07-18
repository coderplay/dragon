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

import java.text.NumberFormat;

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
   * @return the topological index of the task in the task DAG
   */
  public abstract int getIndex();

  /**
   * @return the task number.
   */
  public abstract int getId();

  public abstract void setJobId(JobId jobId);

  public abstract void setIndex(int index);

  public abstract void setId(int id);

  public static final char SEPARATOR = '_';
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
    result = prime * result + getIndex();
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
    if (getIndex() != other.getIndex())
      return false;
    if (getId() != other.getId())
      return false;
    if (!getJobId().equals(other.getJobId()))
      return false;
    return true;
  }
      
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(TASK);
    JobId jobId = getJobId();
    builder.append(SEPARATOR).append(jobId.getAppId().getClusterTimestamp());
    builder.append(SEPARATOR).append(
        JobId.jobIdFormat.get().format(jobId.getAppId().getId()));
    builder.append(SEPARATOR);
    builder.append(taskIndexFormat.get().format(getIndex()));
    builder.append(SEPARATOR);
    builder.append(taskIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(TaskId other) {
    int jobIdComp = this.getJobId().compareTo(other.getJobId());
    if (jobIdComp != 0)
      return jobIdComp;
    int taskIndexComp = this.getIndex() - other.getIndex();
    if (taskIndexComp != 0)
      return taskIndexComp;
    return this.getId() - other.getId();
  }

  public static TaskId forName(String taskName) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }
}