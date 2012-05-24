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

package org.apache.hadoop.realtime;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 */
public interface DragonJobService {

  /**
   * Allocate a name for the job.
   * @return a unique job name for submitting jobs.
   * @throws IOException
   */
  public JobId getNewJobId() throws IOException, InterruptedException;
  
  /**
   * Submit a Job for execution.  Returns the latest profile for
   * that job.
   */
  public boolean submitJob(JobId jobId, String jobSubmitDir, Credentials ts)
      throws IOException, InterruptedException;
  
  /**
   * Grab the system directory path from service provider
   * where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public String getSystemDir() throws IOException, InterruptedException;

  /**
   * Get a hint from the service provider 
   * where job-specific files are to be placed.
   * 
   * @return the directory where job-specific files are to be placed.
   */
  public String getStagingAreaDir() throws IOException, InterruptedException;
  
  
  /**
   * Get the administrators of the given job-queue.
   * This method is for hadoop internal use only.
   * @param queueName
   * @return Queue administrators ACL for the queue to which job is
   *         submitted to
   * @throws IOException
   */
  public AccessControlList getQueueAdmins(String queueName) throws IOException;
  
  /**
   * Kill the indicated job
   */
  public void killJob(JobId jobid) throws IOException, InterruptedException;
  
  /**
   * Kill indicated task attempt.
   * @param taskId the id of the task to kill.
   * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
   * it is just killed, w/o affecting job failure status.  
   */ 
  public boolean killTask(TaskAttemptId taskId, boolean shouldFail) throws IOException, InterruptedException;

  /**
   * Grab a bunch of info on the tasks that make up the job
   */
  public List<TaskReport> getTaskReports(JobId jobid) throws IOException,
      InterruptedException;

  public JobReport getJobReport(JobId jobId) throws YarnRemoteException;
}
