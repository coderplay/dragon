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

package org.apache.hadoop.realtime.records.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.realtime.records.Counters;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.records.TaskState;
import org.apache.hadoop.realtime.util.DragonProtoUtils;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.CountersProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskAttemptIdProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskIdProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskReportProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskReportProtoOrBuilder;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskStateProto;


    
public class TaskReportPBImpl extends ProtoBase<TaskReportProto> implements TaskReport {
  TaskReportProto proto = TaskReportProto.getDefaultInstance();
  TaskReportProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  private Counters counters = null;
  private TaskAttemptId runningAttempt = null;
  private String diagnostics = null;
  
  
  public TaskReportPBImpl() {
    builder = TaskReportProto.newBuilder();
  }

  public TaskReportPBImpl(TaskReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public TaskReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
    if (this.runningAttempt != null) {
      builder.setRunningAttempt(convertToProtoFormat(this.runningAttempt));
    }
    if (this.diagnostics != null) {
      builder.setDiagnostics(this.diagnostics);
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
      builder = TaskReportProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Counters getCounters() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.counters != null) {
      return this.counters;
    }
    if (!p.hasCounters()) {
      return null;
    }
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) 
      builder.clearCounters();
    this.counters = counters;
  }
  @Override
  public long getStartTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }
  
  @Override
  public long getFinishTime() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }
  
  @Override
  public TaskId getTaskId() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    this.taskId = convertFromProtoFormat(p.getTaskId());
    return this.taskId;
  }

  @Override
  public void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null) 
      builder.clearTaskId();
    this.taskId = taskId;
  }

  @Override
  public TaskState getTaskState() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskState()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskState());
  }

  @Override
  public void setTaskState(TaskState taskState) {
    maybeInitBuilder();
    if (taskState == null) {
      builder.clearTaskState();
      return;
    }
    builder.setTaskState(convertToProtoFormat(taskState));
  }
  @Override
  public TaskAttemptId getRunningAttempt() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRunningAttempt()) {
      return null;
    }
    return convertFromProtoFormat(p.getRunningAttempt());
  }
  
  @Override
  public void setRunningAttempt(TaskAttemptId taskAttempt) {
    maybeInitBuilder();
    if (taskAttempt == null) {
      builder.clearRunningAttempt();
      return;
    }
    builder.setRunningAttempt(convertToProtoFormat(taskAttempt));
    
  }
  @Override
  public String getDiagnostics() {
    TaskReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.diagnostics;
    }
    if (!p.hasDiagnostics()) {
      return null;
    }
    this.diagnostics = p.getDiagnostics();
    return this.diagnostics;
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) 
      builder.clearDiagnostics();
    this.diagnostics = diagnostics;
  }

  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }

  private TaskStateProto convertToProtoFormat(TaskState e) {
    return DragonProtoUtils.convertToProtoFormat(e);
  }

  private TaskState convertFromProtoFormat(TaskStateProto e) {
    return DragonProtoUtils.convertFromProtoFormat(e);
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }
}  
