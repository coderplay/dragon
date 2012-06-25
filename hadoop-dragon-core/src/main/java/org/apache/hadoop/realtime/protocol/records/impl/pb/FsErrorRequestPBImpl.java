package org.apache.hadoop.realtime.protocol.records.impl.pb;

import org.apache.hadoop.realtime.protocol.records.FsErrorRequest;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskAttemptIdProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FsErrorRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FsErrorRequestProtoOrBuilder;

public class FsErrorRequestPBImpl extends ProtoBase<FsErrorRequestProto> implements FsErrorRequest{

  FsErrorRequestProto proto = FsErrorRequestProto.getDefaultInstance();
  FsErrorRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskAttemptId taskAttemptId = null;
  private String message = null;
  
  
  public FsErrorRequestPBImpl() {
    builder = FsErrorRequestProto.newBuilder();
  }

  public FsErrorRequestPBImpl(FsErrorRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FsErrorRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FsErrorRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    FsErrorRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    maybeInitBuilder();
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    this.taskAttemptId = taskAttemptId;
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }


  @Override
  public String getMessage() {
    FsErrorRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.message != null) {
      return this.message;
    }
    if (!p.hasMessage()) {
      return null;
    }
    this.message = p.getMessage();
    return this.message;
  }

  @Override
  public void setMessage(String message) {
    maybeInitBuilder();
    if (message == null) 
      builder.clearMessage();
    this.message = message;
  }

}
