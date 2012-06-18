package org.apache.hadoop.realtime.records.impl.pb;

import java.util.Map;

import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskIdProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskInChildProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskInChildProtoOrBuilder;

public class TaskInChildPBImpl extends TaskInChild{

  TaskInChildProto proto = TaskInChildProto.getDefaultInstance();
  TaskInChildProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null; 
  
  public TaskInChildPBImpl() {
    builder = TaskInChildProto.newBuilder(proto);
  }

  public TaskInChildPBImpl(TaskInChildProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized TaskInChildProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.taskId != null
        && !((TaskIdPBImpl) this.taskId).getProto().equals(builder.getTaskId())) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
  }

  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskInChildProto.newBuilder(proto);
    }
    viaProto = false;
  }
  @Override
  public TaskId getID() {
    TaskInChildProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    taskId = convertFromProtoFormat(p.getTaskId());
    return taskId;
  }
  
  public synchronized void setID(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null)
      builder.clearTaskId();
    this.taskId = taskId;
  }
  
  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }

  @Override
  public TaskReport getReport() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskState getState() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Counters getCounters() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getLabel() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<TaskAttemptId, TaskAttempt> getAttempts() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskAttempt getAttempt(TaskAttemptId attemptID) {
    // TODO Auto-generated method stub
    return null;
  }

}
