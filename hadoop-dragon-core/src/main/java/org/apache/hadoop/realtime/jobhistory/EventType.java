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
package org.apache.hadoop.realtime.jobhistory;

/**
 * class description goes here.
 *
 */
public enum EventType {

  //Producer:MRAppMaster
  JOB_INITED,
  JOB_STARTED,

  //Producer:Task
  TASK_COMPLETED,
  TASK_RESCHEDULED,
  TASK_ATTEMPT_COMPLETED,

  //Producer:Job
  JOB_COMPLETED,

  //Producer:Any component
  JOB_DIAGNOSTIC_UPDATED,
  INTERNAL_ERROR_OCCUR,
  JOB_COUNTER_UPDATED,

  //Producer:TaskAttemptListener
  TASK_ATTEMPT_FETCH_FAILURED;
}
