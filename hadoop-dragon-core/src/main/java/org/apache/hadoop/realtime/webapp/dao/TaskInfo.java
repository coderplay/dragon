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
package org.apache.hadoop.realtime.webapp.dao;

import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.yarn.util.Times;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement(name = "task")
@XmlAccessorType(XmlAccessType.FIELD)
public class TaskInfo {

  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected String id;
  protected TaskState state;
  protected String type;

  @XmlTransient
  int taskNum;

  @XmlTransient
  TaskAttempt successful;

  public TaskInfo() {
  }

  public TaskInfo(Task task) {
    TaskType ttype = task.getID().getTaskType();
    this.type = ttype.toString();
    TaskReport report = task.getReport();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.elapsedTime = Times.elapsed(this.startTime, this.finishTime, false);
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.state = report.getTaskState();
    this.id = task.getID().toString();
    this.taskNum = task.getID().getId();
  }

  public String getState() {
    return this.state.toString();
  }

  public String getId() {
    return this.id;
  }

  public int getTaskNum() {
    return this.taskNum;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public TaskAttempt getSuccessful() {
    return this.successful;
  }

}
