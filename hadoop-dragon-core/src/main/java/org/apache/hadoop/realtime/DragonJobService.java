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

import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.Credentials;

/**
 */
public interface DragonJobService {

  /**
   * Allocate a name for the job.
   * @return a unique job name for submitting jobs.
   * @throws IOException
   */
  public JobId getNewJobID() throws IOException, InterruptedException;
  
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

}
