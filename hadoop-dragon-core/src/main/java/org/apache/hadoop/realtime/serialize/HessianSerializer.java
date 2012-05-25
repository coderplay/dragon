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
package org.apache.hadoop.realtime.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import com.caucho.hessian.io.Hessian2StreamingInput;
import com.caucho.hessian.io.Hessian2StreamingOutput;
/**
 * Hessian serializer implemenation.
 */
public final class HessianSerializer<T> implements Serializer<T> {

  @Override
  public void serialize(OutputStream out, T dag) throws IOException {
    Hessian2StreamingOutput hout = new Hessian2StreamingOutput(out);
    hout.writeObject(dag);
  }

  @Override
  public T deserialize(InputStream in) throws IOException {
    Hessian2StreamingInput hin = new Hessian2StreamingInput(in);
    return (T) hin.readObject();
  }
}
