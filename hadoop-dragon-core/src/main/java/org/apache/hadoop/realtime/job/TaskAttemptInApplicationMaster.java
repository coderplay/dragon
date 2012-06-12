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

import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.realtime.records.TaskAttemptState;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

/**
 */
public class TaskAttemptInApplicationMaster implements TaskAttempt {

  private final TaskAttemptId attemptId;

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  TaskAttemptInApplicationMaster(TaskId taskId, int i) {
    attemptId = recordFactory.newRecordInstance(TaskAttemptId.class);
    attemptId.setTaskId(taskId);
    attemptId.setId(i);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getID()
   */
  @Override
  public TaskAttemptId getID() {
    return attemptId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getReport()
   */
  @Override
  public TaskAttemptReport getReport() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getDiagnostics()
   */
  @Override
  public List<String> getDiagnostics() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getCounters()
   */
  @Override
  public Counters getCounters() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getState()
   */
  @Override
  public TaskAttemptState getState() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#isFinished()
   */
  @Override
  public boolean isFinished() {
    // TODO Auto-generated method stub
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getAssignedContainerID()
   */
  @Override
  public ContainerId getAssignedContainerID() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.job.TaskAttempt#getAssignedContainerMgrAddress()
   */
  @Override
  public String getAssignedContainerMgrAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getNodeHttpAddress()
   */
  @Override
  public String getNodeHttpAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getNodeRackName()
   */
  @Override
  public String getNodeRackName() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getLaunchTime()
   */
  @Override
  public long getLaunchTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getFinishTime()
   */
  @Override
  public long getFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getShuffleFinishTime()
   */
  @Override
  public long getShuffleFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getSortFinishTime()
   */
  @Override
  public long getSortFinishTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.job.TaskAttempt#getShufflePort()
   */
  @Override
  public int getShufflePort() {
    // TODO Auto-generated method stub
    return 0;
  }

}
