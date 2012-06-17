package org.apache.hadoop.realtime.protocol.impl.pb.client;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressRequest;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.PingRequest;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetShuffleAddressRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetShuffleAddressResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.PingRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.PingResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.StatusUpdateRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.StatusUpdateResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;
import org.apache.hadoop.yarn.proto.DragonChildProtocol.DragonChildProtocolService;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetShuffleAddressRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.PingRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.StatusUpdateRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskRequestProto;

import com.google.protobuf.ServiceException;

public class DragonChildProtocolPBClientImpl implements DragonChildProtocol {

  protected DragonChildProtocolService.BlockingInterface proxy;

  public DragonChildProtocolPBClientImpl() {
  };

  public DragonChildProtocolPBClientImpl(long clientVersion,
      InetSocketAddress addr, Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf,
        DragonChildProtocolService.BlockingInterface.class,
        ProtoOverHadoopRpcEngine.class);
    proxy =
        (DragonChildProtocolService.BlockingInterface) RPC.getProxy(
            DragonChildProtocolService.BlockingInterface.class, clientVersion,
            addr, conf);
  }

  @Override
  public GetTaskResponse getTask(GetTaskRequest request)
      throws YarnRemoteException {
    GetTaskRequestProto requestProto =
        ((GetTaskRequestPBImpl) request).getProto();
    try {
      return new GetTaskResponsePBImpl(proxy.getTask(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException) e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException) e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public StatusUpdateResponse statusUpdate(StatusUpdateRequest request)
      throws YarnRemoteException {
    StatusUpdateRequestProto requestProto =
        ((StatusUpdateRequestPBImpl) request).getProto();
    try {
      return new StatusUpdateResponsePBImpl(proxy.statusUpdate(null,
          requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException) e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException) e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public PingResponse ping(PingRequest request) throws YarnRemoteException {
    PingRequestProto requestProto = ((PingRequestPBImpl) request).getProto();
    try {
      return new PingResponsePBImpl(proxy.ping(null, requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException) e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException) e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  @Override
  public GetShuffleAddressResponse getShuffleAddress(
      GetShuffleAddressRequest request) throws YarnRemoteException {
    GetShuffleAddressRequestProto requestProto =
        ((GetShuffleAddressRequestPBImpl) request).getProto();
    try {
      return new GetShuffleAddressResponsePBImpl(proxy.getShuffleAddress(null,
          requestProto));
    } catch (ServiceException e) {
      if (e.getCause() instanceof YarnRemoteException) {
        throw (YarnRemoteException) e.getCause();
      } else if (e.getCause() instanceof UndeclaredThrowableException) {
        throw (UndeclaredThrowableException) e.getCause();
      } else {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

}
