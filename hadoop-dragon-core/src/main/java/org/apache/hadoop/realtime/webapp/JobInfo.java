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
package org.apache.hadoop.realtime.webapp;

import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.util.Times;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.Map;

@XmlRootElement(name = "job")
@XmlAccessorType(XmlAccessType.FIELD)
public class JobInfo {

  // ok for any user to see
  protected long startTime;
  protected long finishTime;
  protected long elapsedTime;
  protected String id;
  protected String name;
  protected String user;
  protected JobState state;
  protected int mapsTotal;
  protected int mapsCompleted;
  protected int reducesTotal;
  protected int reducesCompleted;
  protected float mapProgress;
  protected float reduceProgress;

  @XmlTransient
  protected String mapProgressPercent;
  @XmlTransient
  protected String reduceProgressPercent;

  // these should only be seen if acls allow
  protected boolean uberized;
  protected String diagnostics;

  public JobInfo() {
  }

  public JobInfo(Job job, Boolean hasAccess) {
    this.id = DragonApps.toString(job.getID());
    JobReport report = job.getReport();
    this.startTime = report.getStartTime();
    this.finishTime = report.getFinishTime();
    this.elapsedTime = Times.elapsed(this.startTime, this.finishTime);
    if (this.elapsedTime == -1) {
      this.elapsedTime = 0;
    }
    this.name = job.getName().toString();
    this.user = job.getUser();
    this.state = job.getState();
  }

  public String getState() {
    return this.state.toString();
  }

  public String getUserName() {
    return this.user;
  }

  public String getName() {
    return this.name;
  }

  public String getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public long getFinishTime() {
    return this.finishTime;
  }

  public boolean isUberized() {
    return this.uberized;
  }

  public String getdiagnostics() {
    return this.diagnostics;
  }

  public float getMapProgress() {
    return this.mapProgress;
  }

  public String getMapProgressPercent() {
    return this.mapProgressPercent;
  }

  public float getReduceProgress() {
    return this.reduceProgress;
  }

  public String getReduceProgressPercent() {
    return this.reduceProgressPercent;
  }

  /**
   * Go through a job and update the member variables with counts for
   * information to output in the page.
   *
   * @param job
   *          the job to get counts for.
   */
  private void countTasksAndAttempts(Job job) {
    final Map<TaskId, Task> tasks = job.getTasks();
    if (tasks == null) {
      return;
    }
    for (Task task : tasks.values()) {

    }
  }

  public Object getTotalTask() {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public Object getNewAttempts() {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public Object getRunningAttempts() {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public Object getFailedAttempts() {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public Object getKilledAttempts() {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }
}
