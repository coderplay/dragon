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
package org.apache.hadoop.realtime.jobhistory;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.realtime.jobhistory.event.*;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * create and setup kryo utils
 *
 */
public class KryoUtils {

  public static Kryo createHistoryEventKryo() {
    Kryo kryo = new Kryo();

    // we need support arguments constructor
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

    // register all history event
    kryo.register(JobInfoChangeEvent.class);
    kryo.register(JobInitedEvent.class);
    kryo.register(JobSubmittedEvent.class);
    kryo.register(JobUnsuccessfulCompletionEvent.class);
    kryo.register(TaskFailedEvent.class);
    kryo.register(TaskStartedEvent.class);
    kryo.register(TaskAttemptStartedEvent.class);
    kryo.register(TaskAttemptUnsuccessfulCompletionEvent.class);

    return kryo;
  }
}
