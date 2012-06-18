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

import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskRequestProto;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskRequestProtoOrBuilder;

public class GetTaskRequestPBImpl extends ProtoBase<GetTaskRequestProto>
    implements GetTaskRequest {

  GetTaskRequestProto proto = GetTaskRequestProto.getDefaultInstance();
  GetTaskRequestProto.Builder builder = null;
  boolean viaProto = false;

  private String containerId = null;

  public GetTaskRequestPBImpl() {
    builder = GetTaskRequestProto.newBuilder();
  }

  public GetTaskRequestPBImpl(GetTaskRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public GetTaskRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null) {
     builder.setContainerId(this.containerId);
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
      builder = GetTaskRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getContainerId() {
    GetTaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = p.getContainerId().toString();
    return this.containerId;
  }

  @Override
  public void setContainerId(String containerId) {
    maybeInitBuilder();
    if (containerId == null)
      builder.clearContainerId();
    this.containerId = containerId;

  }

}
