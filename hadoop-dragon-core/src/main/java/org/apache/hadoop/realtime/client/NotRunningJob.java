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

package org.apache.hadoop.realtime.client;

import java.util.ArrayList;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.realtime.protocol.DragonClientProtocol;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptResponse;
import org.apache.hadoop.realtime.protocol.records.GetCountersRequest;
import org.apache.hadoop.realtime.protocol.records.GetCountersResponse;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenRequest;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenResponse;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsRequest;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsResponse;
import org.apache.hadoop.realtime.protocol.records.GetJobReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetJobReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskAttemptReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskAttemptReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskReportsResponse;
import org.apache.hadoop.realtime.protocol.records.KillJobRequest;
import org.apache.hadoop.realtime.protocol.records.KillJobResponse;
import org.apache.hadoop.realtime.protocol.records.KillTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.KillTaskAttemptResponse;
import org.apache.hadoop.realtime.protocol.records.KillTaskRequest;
import org.apache.hadoop.realtime.protocol.records.KillTaskResponse;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.JobState;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;

public class NotRunningJob implements DragonClientProtocol {

  private RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final JobState jobState;
  private final ApplicationReport applicationReport;

  private ApplicationReport getUnknownApplicationReport() {
    ApplicationId unknownAppId =
        recordFactory.newRecordInstance(ApplicationId.class);

    // Setting AppState to NEW and finalStatus to UNDEFINED as they are never
    // used
    // for a non running job
    return BuilderUtils.newApplicationReport(unknownAppId, "N/A", "N/A", "N/A",
        "N/A", 0, "", YarnApplicationState.NEW, "N/A", "N/A", 0, 0,
        FinalApplicationStatus.UNDEFINED, null, "N/A");
  }

  NotRunningJob(ApplicationReport applicationReport, JobState jobState) {
    this.applicationReport =
        (applicationReport == null) ? getUnknownApplicationReport()
            : applicationReport;
    this.jobState = jobState;
  }

  @Override
  public FailTaskAttemptResponse
      failTaskAttempt(FailTaskAttemptRequest request)
          throws YarnRemoteException {
    FailTaskAttemptResponse resp =
        recordFactory.newRecordInstance(FailTaskAttemptResponse.class);
    return resp;
  }

  @Override
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
      throws YarnRemoteException {
    GetDiagnosticsResponse resp =
        recordFactory.newRecordInstance(GetDiagnosticsResponse.class);
    resp.addDiagnostics("");
    return resp;
  }

  @Override
  public GetJobReportResponse getJobReport(GetJobReportRequest request)
      throws YarnRemoteException {
    JobReport jobReport = recordFactory.newRecordInstance(JobReport.class);
    jobReport.setJobId(request.getJobId());
    jobReport.setJobState(jobState);
    jobReport.setUser(applicationReport.getUser());
    jobReport.setStartTime(applicationReport.getStartTime());
    jobReport.setDiagnostics(applicationReport.getDiagnostics());
    jobReport.setJobName(applicationReport.getName());
    jobReport.setTrackingUrl(applicationReport.getTrackingUrl());

    GetJobReportResponse resp =
        recordFactory.newRecordInstance(GetJobReportResponse.class);
    resp.setJobReport(jobReport);
    return resp;
  }

  @Override
  public GetTaskAttemptReportResponse getTaskAttemptReport(
      GetTaskAttemptReportRequest request) throws YarnRemoteException {
    // not invoked by anybody
    throw new NotImplementedException();
  }

  @Override
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
      throws YarnRemoteException {
    GetTaskReportResponse resp =
        recordFactory.newRecordInstance(GetTaskReportResponse.class);
    TaskReport report = recordFactory.newRecordInstance(TaskReport.class);
    report.setTaskId(request.getTaskId());
    report.setTaskState(TaskState.NEW);
    resp.setTaskReport(report);
    return resp;
  }

  @Override
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
      throws YarnRemoteException {
    GetTaskReportsResponse resp =
        recordFactory.newRecordInstance(GetTaskReportsResponse.class);
    resp.addAllTaskReports(new ArrayList<TaskReport>());
    return resp;
  }

  @Override
  public KillJobResponse killJob(KillJobRequest request)
      throws YarnRemoteException {
    KillJobResponse resp =
        recordFactory.newRecordInstance(KillJobResponse.class);
    return resp;
  }

  @Override
  public KillTaskResponse killTask(KillTaskRequest request)
      throws YarnRemoteException {
    KillTaskResponse resp =
        recordFactory.newRecordInstance(KillTaskResponse.class);
    return resp;
  }

  @Override
  public KillTaskAttemptResponse
      killTaskAttempt(KillTaskAttemptRequest request)
          throws YarnRemoteException {
    KillTaskAttemptResponse resp =
        recordFactory.newRecordInstance(KillTaskAttemptResponse.class);
    return resp;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    /* Should not be invoked by anyone. */
    throw new NotImplementedException();
  }

  @Override
  public GetCountersResponse getCounters(GetCountersRequest request)
      throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }
}
