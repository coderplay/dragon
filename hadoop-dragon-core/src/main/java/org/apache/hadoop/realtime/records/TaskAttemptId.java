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

public class TaskAttemptId implements Comparable<TaskAttemptId>{

	private TaskId taskId;
	private int id;
	
	public TaskAttemptId(TaskId taskId,int attemptId){
		
	}
	
	public int getId() {
  	return id;
  }
	public void setId(int id) {
  	this.id = id;
  }
	public void setTaskId(TaskId taskId) {
  	this.taskId = taskId;
  }

	public TaskId getTaskId(){
		return taskId;
	}
  protected static final String TASKATTEMPT = "attempt";

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
    builder.append("_").append(
        taskId.getJobId().getAppId().getClusterTimestamp());
    builder.append("_").append(
        JobId.jobIdFormat.get().format(
            getTaskId().getJobId().getAppId().getId()));
    builder.append("_").append(TaskId.taskIdFormat.get().format(taskId.getId()));
    builder.append("_");
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
}

