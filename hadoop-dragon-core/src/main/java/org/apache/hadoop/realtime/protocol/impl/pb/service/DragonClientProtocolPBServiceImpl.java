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

package org.apache.hadoop.realtime.protocol.impl.pb.service;

import org.apache.hadoop.realtime.protocol.DragonClientProtocol;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptRequest;
import org.apache.hadoop.realtime.protocol.records.FailTaskAttemptResponse;
import org.apache.hadoop.realtime.protocol.records.GetCountersRequest;
import org.apache.hadoop.realtime.protocol.records.GetCountersResponse;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenRequest;
import org.apache.hadoop.realtime.protocol.records.GetDelegationTokenResponse;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsRequest;
import org.apache.hadoop.realtime.protocol.records.GetDiagnosticsResponse;
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
import org.apache.hadoop.realtime.protocol.records.impl.pb.FailTaskAttemptRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.FailTaskAttemptResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetCountersRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetCountersResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetDelegationTokenResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetDiagnosticsRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetDiagnosticsResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetJobReportRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetJobReportResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskAttemptReportRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskAttemptReportResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskReportRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskReportResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskReportsRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskReportsResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillJobRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillJobResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillTaskAttemptRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillTaskAttemptResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillTaskRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.KillTaskResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.DragonClientProtocol.DragonClientProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FailTaskAttemptRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FailTaskAttemptResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetCountersRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetCountersResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDiagnosticsResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetJobReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetJobReportResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskAttemptReportResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportsRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportsResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillJobRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillJobResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskAttemptRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskAttemptResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DragonClientProtocolPBServiceImpl implements BlockingInterface {

  private DragonClientProtocol real;
  
  public DragonClientProtocolPBServiceImpl(DragonClientProtocol impl) {
    this.real = impl;
  }
  
  @Override
  public GetJobReportResponseProto getJobReport(RpcController controller,
      GetJobReportRequestProto proto) throws ServiceException {
    GetJobReportRequestPBImpl request = new GetJobReportRequestPBImpl(proto);
    try {
      GetJobReportResponse response = real.getJobReport(request);
      return ((GetJobReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskReportResponseProto getTaskReport(RpcController controller,
      GetTaskReportRequestProto proto) throws ServiceException {
    GetTaskReportRequest request = new GetTaskReportRequestPBImpl(proto);
    try {
      GetTaskReportResponse response = real.getTaskReport(request);
      return ((GetTaskReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskAttemptReportResponseProto getTaskAttemptReport(
      RpcController controller, GetTaskAttemptReportRequestProto proto)
      throws ServiceException {
    GetTaskAttemptReportRequest request = new GetTaskAttemptReportRequestPBImpl(proto);
    try {
      GetTaskAttemptReportResponse response = real.getTaskAttemptReport(request);
      return ((GetTaskAttemptReportResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetCountersResponseProto getCounters(RpcController controller,
      GetCountersRequestProto proto) throws ServiceException {
    GetCountersRequest request = new GetCountersRequestPBImpl(proto);
    try {
      GetCountersResponse response = real.getCounters(request);
      return ((GetCountersResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskReportsResponseProto getTaskReports(RpcController controller,
      GetTaskReportsRequestProto proto) throws ServiceException {
    GetTaskReportsRequest request = new GetTaskReportsRequestPBImpl(proto);
    try {
      GetTaskReportsResponse response = real.getTaskReports(request);
      return ((GetTaskReportsResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetDiagnosticsResponseProto getDiagnostics(RpcController controller,
      GetDiagnosticsRequestProto proto) throws ServiceException {
    GetDiagnosticsRequest request = new GetDiagnosticsRequestPBImpl(proto);
    try {
      GetDiagnosticsResponse response = real.getDiagnostics(request);
      return ((GetDiagnosticsResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public GetDelegationTokenResponseProto getDelegationToken(
      RpcController controller, GetDelegationTokenRequestProto proto)
      throws ServiceException {
    GetDelegationTokenRequest request = new GetDelegationTokenRequestPBImpl(proto);
    try {
      GetDelegationTokenResponse response = real.getDelegationToken(request);
      return ((GetDelegationTokenResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
  @Override
  public KillJobResponseProto killJob(RpcController controller,
      KillJobRequestProto proto) throws ServiceException {
    KillJobRequest request = new KillJobRequestPBImpl(proto);
    try {
      KillJobResponse response = real.killJob(request);
      return ((KillJobResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public KillTaskResponseProto killTask(RpcController controller,
      KillTaskRequestProto proto) throws ServiceException {
    KillTaskRequest request = new KillTaskRequestPBImpl(proto);
    try {
      KillTaskResponse response = real.killTask(request);
      return ((KillTaskResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public KillTaskAttemptResponseProto killTaskAttempt(RpcController controller,
      KillTaskAttemptRequestProto proto) throws ServiceException {
    KillTaskAttemptRequest request = new KillTaskAttemptRequestPBImpl(proto);
    try {
      KillTaskAttemptResponse response = real.killTaskAttempt(request);
      return ((KillTaskAttemptResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public FailTaskAttemptResponseProto failTaskAttempt(RpcController controller,
      FailTaskAttemptRequestProto proto) throws ServiceException {
    FailTaskAttemptRequest request = new FailTaskAttemptRequestPBImpl(proto);
    try {
      FailTaskAttemptResponse response = real.failTaskAttempt(request);
      return ((FailTaskAttemptResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }
  
}
