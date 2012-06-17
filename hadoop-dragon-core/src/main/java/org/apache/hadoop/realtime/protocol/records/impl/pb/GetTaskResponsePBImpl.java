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

import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.realtime.records.impl.pb.TaskInChildPBImpl;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.TaskInChildProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProtoOrBuilder;

public class GetTaskResponsePBImpl extends ProtoBase<GetTaskResponseProto>
    implements GetTaskResponse {
  GetTaskResponseProto proto = GetTaskResponseProto.getDefaultInstance();
  GetTaskResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskInChild task = null;
  
  
  public GetTaskResponsePBImpl() {
    builder = GetTaskResponseProto.newBuilder();
  }

  public GetTaskResponsePBImpl(GetTaskResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void mergeLocalToBuilder() {
    if (this.task != null) {
      builder.setTask(convertToProtoFormat(this.task));
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
      builder = GetTaskResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public TaskInChild getTask() {
    GetTaskResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.task != null) {
      return this.task;
    }
    if (!p.hasTask()) {
      return null;
    }
    this.task =  convertFromProtoFormat(p.getTask());
    return this.task;
  }

  @Override
  public void setTask(TaskInChild task) {
    maybeInitBuilder();
    if (task == null) 
      builder.clearTask();
    this.task = task;
  }

  @Override
  public GetTaskResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private TaskInChildPBImpl convertFromProtoFormat(TaskInChildProto p) {
    return new TaskInChildPBImpl(p);
  }

  private TaskInChildProto convertToProtoFormat(TaskInChild t) {
    return ((TaskInChildPBImpl)t).getProto();
  }
}