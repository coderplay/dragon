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
package org.apache.hadoop.realtime.webapp;


import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.AMInfo;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * mock web server for test
 */
public class DragonWebAppTest {

  public void runMockDragonWebApp() {
    final AppContext appContext = mock(AppContext.class);
    final Job job = mock(Job.class);
    final JobReport report = mock(JobReport.class);
    final JobId jobId = mock(JobId.class);
    final ApplicationId appId = mock(ApplicationId.class);
    final AMInfo amInfo = mock(AMInfo.class);
    final ContainerId containerId = mock(ContainerId.class);
    final ApplicationAttemptId attemptId = mock(ApplicationAttemptId.class);

    when(appContext.getAllJobs()).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Map<JobId, Job> jobMap = new HashMap<JobId, Job>();
        jobMap.put(jobId, job);
        return jobMap;
      }
    });
    when(appContext.getApplicationID()).thenReturn(appId);
    when(appContext.getJob(any(JobId.class))).thenReturn(job);
    when(appContext.getApplicationName()).thenReturn("app name");
    when(appContext.getUser()).thenReturn("user");

    when(job.getID()).thenReturn(jobId);
    when(job.getName()).thenReturn("test job name");
    when(job.getUser()).thenReturn("metis");
    when(job.getState()).thenReturn(JobState.RUNNING);
    when(job.getReport()).thenReturn(report);
    when(job.getAMInfos()).thenReturn(newArrayList(amInfo));

    when(jobId.getAppId()).thenReturn(appId);
    when(jobId.getId()).thenReturn(54321);
    when(jobId.toString()).thenReturn("job_323293_12345_54321");

    when(appId.getId()).thenReturn(12345);
    when(appId.getClusterTimestamp()).thenReturn(323293L);
    when(appId.toString()).thenReturn("application_323293_12345");

    when(amInfo.getNodeManagerHost()).thenReturn("node manager host");
    when(amInfo.getNodeManagerHttpPort()).thenReturn(8000);
    when(amInfo.getContainerId()).thenReturn(containerId);
    when(amInfo.getAppAttemptId()).thenReturn(attemptId);

    when(attemptId.getAttemptId()).thenReturn(9123);
    when(attemptId.toString()).thenReturn("attemp_323293_12345_54321_map_9123_0");

    when(containerId.toString()).thenReturn("888876");


    WebApps.$for("dragon", AppContext.class, appContext)
        .inDevMode().at(8888).start(new DragonWebApp()).joinThread();
  }

  public static void main(String[] args) {
    new DragonWebAppTest().runMockDragonWebApp();
  }
}
