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
package org.apache.hadoop.realtime.event;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 */
public class DirectEventEmitter<KEY, VALUE> implements EventEmitter<KEY, VALUE> {

  @Override
  public boolean emitEvent(Event<KEY, VALUE> event) throws IOException,
      InterruptedException {
    return false;
  }

  @Override
  public boolean
      emitEvent(Event<KEY, VALUE> event, long timeout, TimeUnit unit)
          throws IOException, InterruptedException {
    return false;
  }

  @Override
  public void close() throws IOException {

  }

}
