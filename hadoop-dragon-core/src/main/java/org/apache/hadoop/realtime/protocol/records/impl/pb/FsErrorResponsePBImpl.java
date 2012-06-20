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

import org.apache.hadoop.realtime.protocol.records.FsErrorResponse;
import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.DragonServiceProtos.FsErrorResponseProto;

public class FsErrorResponsePBImpl extends ProtoBase<FsErrorResponseProto>
    implements FsErrorResponse {

  FsErrorResponseProto proto = FsErrorResponseProto.getDefaultInstance();
  FsErrorResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public FsErrorResponsePBImpl() {
    builder = FsErrorResponseProto.newBuilder();
  }

  public FsErrorResponsePBImpl(FsErrorResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public FsErrorResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FsErrorResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

}
