/*
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
package org.apache.hadoop.realtime.app.metrics;


import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;

@Metrics(about="Dragon App Metrics", context="dragon")
public class DragonAppMetrics {
  @Metric MutableCounterInt jobsSubmitted;
  @Metric MutableCounterInt jobsCompleted;
  @Metric MutableCounterInt jobsFailed;
  @Metric MutableCounterInt jobsKilled;
  @Metric MutableGaugeInt jobsPreparing;
  @Metric MutableGaugeInt jobsRunning;

  @Metric MutableCounterInt tasksLaunched;
  @Metric MutableCounterInt tasksFailed;
  @Metric MutableCounterInt tasksKilled;
  @Metric MutableGaugeInt tasksRunning;
  @Metric MutableGaugeInt tasksWaiting;
  
  public static DragonAppMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  public static DragonAppMetrics create(MetricsSystem ms) {
    JvmMetrics.initSingleton("MRAppMaster", null);
    return ms.register(new DragonAppMetrics());
  }

  // potential instrumentation interface methods
  public void submittedJob(Job job) {
    jobsSubmitted.incr();
  }

  public void completedJob(Job job) {
    jobsCompleted.incr();
  }

  public void failedJob(Job job) {
    jobsFailed.incr();
  }

  public void killedJob(Job job) {
    jobsKilled.incr();
  }

  public void preparingJob(Job job) {
    jobsPreparing.incr();
  }

  public void endPreparingJob(Job job) {
    jobsPreparing.decr();
  }

  public void runningJob(Job job) {
    jobsRunning.incr();
  }

  public void endRunningJob(Job job) {
    jobsRunning.decr();
  }

  public void launchedTask(Task task) {
    tasksLaunched.incr();
    endWaitingTask(task);
  }


  public void failedTask(Task task) {
    tasksFailed.incr();
  }

  public void killedTask(Task task) {
    tasksKilled.incr();
  }

  public void runningTask(Task task) {
    tasksRunning.incr();
  }

  public void endRunningTask(Task task) {
    tasksRunning.decr();
  }

  public void waitingTask(Task task) {
    tasksWaiting.incr();
  }

  public void endWaitingTask(Task task) {
    tasksWaiting.decr();
  }
}