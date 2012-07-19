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
package org.apache.hadoop.realtime.serialize;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DragonBuilderUtilsTest {

  // application id = job id
  private static final int jobId = 1;
  private static final int taskId = 2;
  private static final int attemptId = 3;

  private ApplicationId app;

  @Before
  public void setUp() {
    app = BuilderUtils.newApplicationId(System.currentTimeMillis(), jobId);
  }

  @Test
  public void testJobId() {
    JobId job1 = DragonBuilderUtils.newJobId(app, jobId);
    assertEquals(job1.getId(), jobId);

    JobId job2 = DragonBuilderUtils.newJobId(job1.toString());
    assertEquals(job2.getId(), jobId);

    boolean exception = false;
    try {
      DragonBuilderUtils.newJobId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testTaskId() {
    JobId job = DragonBuilderUtils.newJobId(app, jobId);
    TaskId task1 = DragonBuilderUtils.newTaskId(job, taskId);
    assertEquals(task1.getId(), taskId);

    TaskId task2 = DragonBuilderUtils.newTaskId(task1.toString());
    assertEquals(task2.getId(), taskId);

    boolean exception = false;
    try {
      DragonBuilderUtils.newTaskId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testTaskAttemptId() {
    JobId job = DragonBuilderUtils.newJobId(app, jobId);
    TaskId task = DragonBuilderUtils.newTaskId(job, taskId);
    TaskAttemptId attempt1 =
        DragonBuilderUtils.newTaskAttemptId(task, attemptId);
    assertEquals(attempt1.getId(), attemptId);

    System.out.println(attempt1.toString());
    TaskAttemptId attempt2 =
        DragonBuilderUtils.newTaskAttemptId(attempt1.toString());
    assertEquals(attempt2.getId(), attemptId);

    boolean exception = false;
    try {
      DragonBuilderUtils.newTaskAttemptId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }
  
  @Test
  public void testContainerId() {
    JobId job = DragonBuilderUtils.newJobId(app, jobId);
    TaskId task = DragonBuilderUtils.newTaskId(job, taskId);
    TaskAttemptId attempt1 =
        DragonBuilderUtils.newTaskAttemptId(task, attemptId);
    assertEquals(attempt1.getId(), attemptId);

    TaskAttemptId attempt2 =
        DragonBuilderUtils.newTaskAttemptId(attempt1.toString());
    assertEquals(attempt2.getId(), attemptId);

    boolean exception = false;
    try {
      DragonBuilderUtils.newTaskAttemptId("bad format");
    } catch (IllegalArgumentException ignore) {
      exception = true;
    }
    assertTrue(exception);
  }

  @After
  public void tearDown() {
    app = null;
  }
  
  public static void main(String args[]){
    DragonBuilderUtilsTest a = new DragonBuilderUtilsTest();
    a.setUp();
    a.testTaskAttemptId();
  }

}
