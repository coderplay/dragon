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

package org.apache.hadoop.realtime.job;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskId;


/**
 * Main interface to interact with the job. Provides only getters. 
 */
public interface IJobInApp {

  JobId getID();
  String getName();
  JobState getState();
  JobReport getReport();

  /**
   * Get all the counters of this job. This includes job-counters aggregated
   * together with the counters of each task. This creates a clone of the
   * Counters, so use this judiciously.  
   * @return job-counters and aggregate task-counters
   */
  Counters getAllCounters();

  Map<TaskId,Task> getTasks();
  Task getTask(TaskId taskID);
  List<String> getDiagnostics();
  float getProgress();
  String getUserName();
  String getQueueName();
  
  /**
   * @return a path to where the config file for this job is located.
   */
  Path getConfFile();
  
  /**
   * @return the ACLs for this job for each type of JobACL given. 
   */
  // TODO:
  //Map<JobACL, AccessControlList> getJobACLs();

  /**
   * @return information for MR AppMasters (previously failed and current)
   */
  List<AMInfo> getAMInfos();
  
  // TODO:
  //boolean checkAccess(UserGroupInformation callerUGI, JobACL jobOperation);
}
