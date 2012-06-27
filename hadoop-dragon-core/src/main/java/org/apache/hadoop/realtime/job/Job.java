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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJobGraph;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.security.Credentials;

/**
 */
public interface Job {

  /**
   * Get the unique ID for the job.
   * 
   * @return the object with the job id
   */
  public JobId getID();

  /**
   * Get the user-specified job name. This is only used to identify the job to
   * the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getName();
  
  
  public JobState getState();

  /**
   * Get the user-specified queue name. This is only used to identify the job to
   * the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getQueueName();

  /**
   * Get credentials for the job.
   * 
   * @return credentials for the job
   */
  public Credentials getCredentials();

  /**
   * Return the configuration for the job.
   * 
   * @return the shared configuration object
   */
  public Configuration getConfiguration();
  
  
  /**
   * Return the job graph for the job
   * 
   * @return the graph describes job's task topology
   * @return
   */
  public DragonJobGraph getJobGraph();

  /**
   * Get the reported username for this job.
   * 
   * @return the username
   */
  public String getUser();
  

  /**
   * Get all the tasks for this job.
   * 
   * @return the tasks for this job
   */
  Map<TaskId,Task> getTasks();

  
  Map<TaskId,Task> getTasks(String label);
  
  Task getTask(TaskId taskID);
  
  JobReport getReport();
  
  List<String> getDiagnostics();

  Counters getAllCounters();
}
