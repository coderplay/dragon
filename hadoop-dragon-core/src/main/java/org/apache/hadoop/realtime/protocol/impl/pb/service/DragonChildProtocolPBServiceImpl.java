package org.apache.hadoop.realtime.protocol.impl.pb.service;

import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressResponse;
import org.apache.hadoop.realtime.protocol.records.impl.pb.StatusUpdateRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.StatusUpdateResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.PingRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.PingResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetTaskResponsePBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetShuffleAddressRequestPBImpl;
import org.apache.hadoop.realtime.protocol.records.impl.pb.GetShuffleAddressResponsePBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.proto.DragonChildProtocol.DragonChildProtocolService.BlockingInterface;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetShuffleAddressRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetShuffleAddressResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.PingRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.PingResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.StatusUpdateRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.StatusUpdateResponseProto;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DragonChildProtocolPBServiceImpl implements BlockingInterface {

  private DragonChildProtocol real;

  public DragonChildProtocolPBServiceImpl(DragonChildProtocol impl) {
    this.real = impl;
  }

  @Override
  public GetShuffleAddressResponseProto getShuffleAddress(
      RpcController controller, GetShuffleAddressRequestProto proto)
      throws ServiceException {
    GetShuffleAddressRequestPBImpl request =
        new GetShuffleAddressRequestPBImpl(proto);
    try {
      GetShuffleAddressResponse response = real.getShuffleAddress(request);
      return ((GetShuffleAddressResponsePBImpl) response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetTaskResponseProto getTask(RpcController controller,
      GetTaskRequestProto proto) throws ServiceException {
    GetTaskRequestPBImpl request = new GetTaskRequestPBImpl(proto);
    try {
      GetTaskResponse response = real.getTask(request);
      return ((GetTaskResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public PingResponseProto ping(RpcController controller,
      PingRequestProto proto) throws ServiceException {
    PingRequestPBImpl request = new PingRequestPBImpl(proto);
    try {
      PingResponse response = real.ping(request);
      return ((PingResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public StatusUpdateResponseProto statusUpdate(RpcController controller,
      StatusUpdateRequestProto proto) throws ServiceException {
    StatusUpdateRequestPBImpl request = new StatusUpdateRequestPBImpl(proto);
    try {
      StatusUpdateResponse response = real.statusUpdate(request);
      return ((StatusUpdateResponsePBImpl)response).getProto();
    } catch (YarnRemoteException e) {
      throw new ServiceException(e);
    }
  }

}
