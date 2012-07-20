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

import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.ChildExecutionContextProto;
import org.apache.hadoop.yarn.proto.DragonProtos.ChildExecutionContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskAttemptIdProto;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.realtime.util.DragonProtoUtils;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskTypeProto;
/**
 * {@link ChildExecutionContext} protobuf implementation.
 */
public class ChildExecutionContextPBImpl extends
    ProtoBase<ChildExecutionContextProto> implements ChildExecutionContext {

  ChildExecutionContextProto proto = ChildExecutionContextProto
      .getDefaultInstance();
  ChildExecutionContextProto.Builder builder = null;

  boolean viaProto = false;

  private TaskAttemptId attemptId = null;

  private TaskType taskType = null;

  private int partition = 0;

  private String user = null;

  public ChildExecutionContextPBImpl() {
    builder = ChildExecutionContextProto.newBuilder(proto);
  }

  public ChildExecutionContextPBImpl(ChildExecutionContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public synchronized ChildExecutionContextProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private synchronized void mergeLocalToBuilder() {
    if (this.attemptId != null
        && !((TaskAttemptIdPBImpl) this.attemptId).getProto().equals(
            builder.getAttemptId())) {
          setTaskAttemptId(this.attemptId);
      setTaskType(this.taskType);
      setUser(this.user);
      setPartition(this.partition);
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
      builder = ChildExecutionContextProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.records.ChildExecutionContext#getTaskAttemptId()
   */
  @Override
  public TaskAttemptId getTaskAttemptId() {
    ChildExecutionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.attemptId != null) {
      return this.attemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    attemptId = convertFromProtoFormat(p.getAttemptId());
    return attemptId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.records.ChildExecutionContext#setTaskAttemptId
   * (org.apache.hadoop.realtime.records.TaskAttemptId)
   */
  @Override
  public void setTaskAttemptId(TaskAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null)
      builder.clearAttemptId();
    this.attemptId = attemptId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.records.ChildExecutionContext#getPartition()
   */
  @Override
  public int getPartition() {
    ChildExecutionContextProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getPartition());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.records.ChildExecutionContext#setPartition(int)
   */
  @Override
  public void setPartition(int partition) {
    maybeInitBuilder();
    builder.setPartition(partition);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.realtime.records.ChildExecutionContext#getUser()
   */
  @Override
  public String getUser() {
    ChildExecutionContextProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getUser());
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.realtime.records.ChildExecutionContext#setUser(java.lang
   * .String)
   */
  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    builder.setUser(user);
  }

  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl) t).getProto();
  }

  @Override
  public void setTaskType(TaskType taskType) {
    maybeInitBuilder();
    if (taskType == null) {
      builder.clearTaskType();
      return;
    }
    builder.setTaskType(convertToProtoFormat(taskType));
  }

  @Override
  public synchronized TaskType getTaskType() {
    ChildExecutionContextProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskType()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskType());
  }
  private TaskTypeProto convertToProtoFormat(TaskType e) {
    return DragonProtoUtils.convertToProtoFormat(e);
  }

  private TaskType convertFromProtoFormat(TaskTypeProto e) {
    return DragonProtoUtils.convertFromProtoFormat(e);
  }
  

}
