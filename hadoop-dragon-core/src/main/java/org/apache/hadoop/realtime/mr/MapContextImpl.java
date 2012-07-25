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

package org.apache.hadoop.realtime.mr;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskType;

/**
 * The context that is given to the {@link Mapper}.
 * 
 * @param <KEYIN> the key input type to the Mapper
 * @param <VALUEIN> the value input type to the Mapper
 * @param <KEYOUT> the key output type from the Mapper
 * @param <VALUEOUT> the value output type from the Mapper
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  @Override
  public DragonConfiguration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getPartition() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getUser() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TaskType getTaskType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent() throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event) throws IOException,
      InterruptedException {
    return false;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event, long timeout,
      TimeUnit unit) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return false;
  }

}
