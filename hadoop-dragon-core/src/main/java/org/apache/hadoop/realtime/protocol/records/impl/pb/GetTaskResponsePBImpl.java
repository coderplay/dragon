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

import java.util.List;

import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.GetTaskResponseProto;

public class GetTaskResponsePBImpl extends ProtoBase<GetTaskResponseProto>
    implements GetTaskResponse {
  GetTaskResponseProto proto = GetTaskResponseProto.getDefaultInstance();
  GetTaskResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private List<Task> task = null;
  
  
  public GetTaskResponsePBImpl() {
    builder = GetTaskResponseProto.newBuilder();
  }

  public GetTaskResponsePBImpl(GetTaskResponseProto proto) {
    this.proto = proto;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTask(TaskInChild task) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public GetTaskResponseProto getProto() {
    // TODO Auto-generated method stub
    return null;
  }
}