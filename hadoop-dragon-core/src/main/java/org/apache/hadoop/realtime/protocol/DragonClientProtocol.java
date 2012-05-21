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

package org.apache.hadoop.realtime.protocol;

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
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface DragonClientProtocol {
  public GetJobReportResponse getJobReport(GetJobReportRequest request) throws YarnRemoteException;
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request) throws YarnRemoteException;
  public GetTaskAttemptReportResponse getTaskAttemptReport(GetTaskAttemptReportRequest request) throws YarnRemoteException;
  public GetCountersResponse getCounters(GetCountersRequest request) throws YarnRemoteException;
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request) throws YarnRemoteException;
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request) throws YarnRemoteException;
  public KillJobResponse killJob(KillJobRequest request) throws YarnRemoteException;
  public KillTaskResponse killTask(KillTaskRequest request) throws YarnRemoteException;
  public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request) throws YarnRemoteException;
  public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request) throws YarnRemoteException;
  public GetDelegationTokenResponse getDelegationToken(GetDelegationTokenRequest request) throws YarnRemoteException;
}
