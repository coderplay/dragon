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

package org.apache.hadoop.realtime.protocol.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
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
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.DragonClientProtocol.DragonClientProtocolService;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FailTaskAttemptRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetCountersRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetJobReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskAttemptReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskReportsRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillJobRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskAttemptRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.KillTaskRequestProto;

import com.google.protobuf.ServiceException;

public class DragonClientProtocolPBClientImpl implements DragonClientProtocol {

  protected DragonClientProtocolService.BlockingInterface proxy;
  
  public DragonClientProtocolPBClientImpl() {};
  
  public DragonClientProtocolPBClientImpl(long clientVersion, InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, DragonClientProtocolService.BlockingInterface.class, ProtoOverHadoopRpcEngine.class);
    proxy = (DragonClientProtocolService.BlockingInterface)RPC.getProxy(
        DragonClientProtocolService.BlockingInterface.class, clientVersion, addr, conf);
  }
  
  @Override
  public GetJobReportResponse getJobReport(GetJobReportRequest request)
      throws YarnRemoteException {
    GetJobReportRequestProto requestProto = ((GetJobReportRequestPBImpl)request).getProto();
    try {
      return new GetJobReportResponsePBImpl(proxy.getJobReport(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetTaskReportResponse getTaskReport(GetTaskReportRequest request)
      throws YarnRemoteException {
    GetTaskReportRequestProto requestProto = ((GetTaskReportRequestPBImpl)request).getProto();
    try {
      return new GetTaskReportResponsePBImpl(proxy.getTaskReport(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetTaskAttemptReportResponse getTaskAttemptReport(
      GetTaskAttemptReportRequest request) throws YarnRemoteException {
    GetTaskAttemptReportRequestProto requestProto = ((GetTaskAttemptReportRequestPBImpl)request).getProto();
    try {
      return new GetTaskAttemptReportResponsePBImpl(proxy.getTaskAttemptReport(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetCountersResponse getCounters(GetCountersRequest request)
      throws YarnRemoteException {
    GetCountersRequestProto requestProto = ((GetCountersRequestPBImpl)request).getProto();
    try {
      return new GetCountersResponsePBImpl(proxy.getCounters(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetTaskReportsResponse getTaskReports(GetTaskReportsRequest request)
      throws YarnRemoteException {
    GetTaskReportsRequestProto requestProto = ((GetTaskReportsRequestPBImpl)request).getProto();
    try {
      return new GetTaskReportsResponsePBImpl(proxy.getTaskReports(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetDiagnosticsResponse getDiagnostics(GetDiagnosticsRequest request)
      throws YarnRemoteException {
    GetDiagnosticsRequestProto requestProto = ((GetDiagnosticsRequestPBImpl)request).getProto();
    try {
      return new GetDiagnosticsResponsePBImpl(proxy.getDiagnostics(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
  
  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    GetDelegationTokenRequestProto requestProto = ((GetDelegationTokenRequestPBImpl)
        request).getProto();
    try {
      return new GetDelegationTokenResponsePBImpl(proxy.getDelegationToken(
          null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
  
  @Override
  public KillJobResponse killJob(KillJobRequest request)
      throws YarnRemoteException {
    KillJobRequestProto requestProto = ((KillJobRequestPBImpl)request).getProto();
    try {
      return new KillJobResponsePBImpl(proxy.killJob(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public KillTaskResponse killTask(KillTaskRequest request)
      throws YarnRemoteException {
    KillTaskRequestProto requestProto = ((KillTaskRequestPBImpl)request).getProto();
    try {
      return new KillTaskResponsePBImpl(proxy.killTask(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public KillTaskAttemptResponse killTaskAttempt(KillTaskAttemptRequest request)
      throws YarnRemoteException {
    KillTaskAttemptRequestProto requestProto = ((KillTaskAttemptRequestPBImpl)request).getProto();
    try {
      return new KillTaskAttemptResponsePBImpl(proxy.killTaskAttempt(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public FailTaskAttemptResponse failTaskAttempt(FailTaskAttemptRequest request)
      throws YarnRemoteException {
    FailTaskAttemptRequestProto requestProto = ((FailTaskAttemptRequestPBImpl)request).getProto();
    try {
      return new FailTaskAttemptResponsePBImpl(proxy.failTaskAttempt(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException)e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException)e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
  
}
