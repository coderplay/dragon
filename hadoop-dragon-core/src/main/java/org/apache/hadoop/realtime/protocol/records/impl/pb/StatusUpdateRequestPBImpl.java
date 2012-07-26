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

package org.apache.hadoop.realtime.protocol.records.impl.pb;

import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.realtime.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.realtime.records.impl.pb.TaskAttemptReportPBImpl;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskAttemptIdProto;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskAttemptReportProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.StatusUpdateRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.StatusUpdateRequestProtoOrBuilder;

public class StatusUpdateRequestPBImpl extends
    ProtoBase<StatusUpdateRequestProto> implements StatusUpdateRequest {
  StatusUpdateRequestProto proto = StatusUpdateRequestProto
      .getDefaultInstance();
  StatusUpdateRequestProto.Builder builder = null;
  boolean viaProto = false;

  private TaskAttemptId attemptId = null;
  private TaskAttemptReport report = null;

  public StatusUpdateRequestPBImpl() {
    builder = StatusUpdateRequestProto.newBuilder();
  }

  public StatusUpdateRequestPBImpl(StatusUpdateRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public StatusUpdateRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.attemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.attemptId));
    }
    if (this.report != null) {
      builder.setTaskAttemptReport(convertToProtoFormat(this.report));
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
      builder = StatusUpdateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    StatusUpdateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.attemptId != null) {
      return this.attemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    this.attemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.attemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) 
      builder.clearTaskAttemptId();
    this.attemptId = attemptId;
  }

  @Override
  public TaskAttemptReport getTaskAttemptReport() {
    StatusUpdateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.report != null) {
      return this.report;
    }
    if (!p.hasTaskAttemptReport()) {
      return null;
    }
    this.report = convertFromProtoFormat(p.getTaskAttemptReport());
    return this.report;
  }

  @Override
  public void setTaskStatus(TaskAttemptReport TaskAttemptReport) {
    maybeInitBuilder();
    if (report == null) 
      builder.clearTaskAttemptReport();
    this.report = TaskAttemptReport;
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl) t).getProto();
  }

  private TaskAttemptReportPBImpl convertFromProtoFormat(TaskAttemptReportProto p) {
    return new TaskAttemptReportPBImpl(p);
  }

  private TaskAttemptReportProto convertToProtoFormat(TaskAttemptReport t) {
    return ((TaskAttemptReportPBImpl) t).getProto();
  }

}
