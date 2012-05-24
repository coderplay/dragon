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
package org.apache.hadoop.realtime.server;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.security.Credentials;

/**
 */
public class JobInApplicationMaster implements Job {

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getJobId()
   */
  @Override
  public JobId getJobId() {
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getJobName()
   */
  @Override
  public String getJobName() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getQueue()
   */
  @Override
  public String getQueue() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getCredentials()
   */
  @Override
  public Credentials getCredentials() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getConfiguration()
   */
  @Override
  public Configuration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.realtime.job.Job#getUser()
   */
  @Override
  public String getUser() {
    // TODO Auto-generated method stub
    return null;
  }
  
  public Task getTask(TaskId taskId){
    return null;  
  }

}
