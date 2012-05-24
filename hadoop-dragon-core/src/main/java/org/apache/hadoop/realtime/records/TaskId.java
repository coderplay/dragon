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
 */
public class TaskId implements Comparable<TaskId>{

  private JobId jobId;
  private int id;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public TaskId(JobId jobId, int id) {
  }

  public JobId getJobId() {
    return jobId;
  }
  protected static final String TASK = "task";

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
  public String toString() {
    StringBuilder builder = new StringBuilder(TASK);
    JobId jobId = getJobId();
    builder.append("_").append(jobId.getAppId().getClusterTimestamp());
    builder.append("_").append(
        JobId.jobIdFormat.get().format(jobId.getAppId().getId()));
    builder.append("_");
    builder.append(taskIdFormat.get().format(getId()));
    return builder.toString();
  }

  @Override
  public int compareTo(TaskId other) {
    int jobIdComp = this.getJobId().compareTo(other.getJobId());
    if (jobIdComp == 0) {
        return this.getId() - other.getId();
    } else {
      return jobIdComp;
    }
  }
}
