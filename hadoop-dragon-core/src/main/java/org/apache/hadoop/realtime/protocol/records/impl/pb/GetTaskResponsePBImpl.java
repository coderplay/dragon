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
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonProtos.ChildExecutionContextProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProtoOrBuilder;

public class GetTaskResponsePBImpl extends ProtoBase<GetTaskResponseProto>
    implements GetTaskResponse {
  GetTaskResponseProto proto = GetTaskResponseProto.getDefaultInstance();
  GetTaskResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private ChildExecutionContext context = null;
  
  
  public GetTaskResponsePBImpl() {
    builder = GetTaskResponseProto.newBuilder();
  }

  public GetTaskResponsePBImpl(GetTaskResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  private void mergeLocalToBuilder() {
    if (this.context != null) {
      builder.setContext(convertToProtoFormat(this.context));
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
  public ChildExecutionContext getTask() {
    GetTaskResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.context != null) {
      return this.context;
    }
    if (!p.hasContext()) {
      return null;
    }
    this.context = convertFromProtoFormat(p.getContext());
    return this.context;
  }

  @Override
  public void setTask(ChildExecutionContext context) {
    maybeInitBuilder();
    if (context == null) 
      builder.clearContext();
    this.context = context;
  }

  @Override
  public GetTaskResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  private ChildExecutionContextPBImpl convertFromProtoFormat(
      ChildExecutionContextProto p) {
    return new ChildExecutionContextPBImpl(p);
  }

  private ChildExecutionContextProto convertToProtoFormat(
      ChildExecutionContext t) {
    return ((ChildExecutionContextPBImpl) t).getProto();
  }
}