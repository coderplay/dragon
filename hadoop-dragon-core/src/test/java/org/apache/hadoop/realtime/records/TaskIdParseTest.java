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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TaskIdParseTest {

  // application id = job id
  private static final int jobId = 1;
  private static final int taskId = 2;
  private static final TaskType taskType = TaskType.valueOf("MAP");
  private static final int attemptId = 3;

  private ApplicationId appId;

  @Before
  public void setUp() {
    appId = BuilderUtils.newApplicationId(System.currentTimeMillis(), jobId);
  }

  @Test
  public void testJobId() {
    JobId job1 = JobId.newJobId(appId, jobId);
    assertEquals(job1.getId(), jobId);
    System.out.println(job1);

    JobId job2 = JobId.parseJobId(job1.toString());
    assertEquals(job2.getId(), jobId);

    boolean exception = false;
    try {
      JobId.parseJobId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testTaskId() {
    JobId job = JobId.newJobId(appId, jobId);
    TaskId task1 = TaskId.newTaskId(job, taskId,taskType);
    assertEquals(task1.getId(), taskId);

    TaskId task2 = TaskId.parseTaskId(task1.toString());
    assertEquals(task2.getId(), taskId);

    boolean exception = false;
    try {
      TaskId.parseTaskId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testTaskAttemptId() {
    JobId job = JobId.newJobId(appId, jobId);
    TaskId task = TaskId.newTaskId(job, taskId,taskType);
    TaskAttemptId attempt1 =
        TaskAttemptId.newTaskAttemptId(task, attemptId);
    assertEquals(attempt1.getId(), attemptId);

    System.out.println(attempt1.toString());
    TaskAttemptId attempt2 =
        TaskAttemptId.parseTaskAttemptId(attempt1.toString());
    assertEquals(attempt2.getId(), attemptId);

    boolean exception = false;
    try {
      TaskAttemptId.parseTaskAttemptId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }
  
  @Test
  public void testContainerId() {
    JobId job = JobId.newJobId(appId, jobId);
    TaskId task = TaskId.newTaskId(job, taskId,taskType);
    TaskAttemptId attempt1 =
        TaskAttemptId.newTaskAttemptId(task, attemptId);
    assertEquals(attempt1.getId(), attemptId);

    TaskAttemptId attempt2 =
        TaskAttemptId.parseTaskAttemptId(attempt1.toString());
    assertEquals(attempt2.getId(), attemptId);

    boolean exception = false;
    try {
      TaskAttemptId.parseTaskAttemptId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @After
  public void tearDown() {
    appId = null;
  }
  
  public static void main(String args[]){
    TaskIdParseTest a = new TaskIdParseTest();
    a.setUp();
    a.testTaskAttemptId();
  }

}
