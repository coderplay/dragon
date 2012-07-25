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

package org.apache.hadoop.realtime.child;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.mr.Reducer;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskType;

/**
 * A context object that allows input and output from the task. It is only
 * supplied to the {@link Mapper} or {@link Reducer}.
 * @param <KEYIN> the input key type for the task
 * @param <VALUEIN> the input value type for the task
 * @param <KEYOUT> the output key type for the task
 * @param <VALUEOUT> the output value type for the task
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Context<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  
  DragonConfiguration getConfiguration();

  TaskAttemptId getTaskAttemptId();

  /**
   * return the partition number of this task
   * @return
   */
  int getPartition();

  String getUser();


  TaskType getTaskType();

  /**
   * take a event from the queue.
   * 
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  public Event<KEYIN, VALUEIN> pollEvent() throws IOException,
      InterruptedException;

  /**
   * take a event from event producer
   * 
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException;

  /**
   * Generate an output event
   */
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event) throws IOException,
      InterruptedException;

  /**
   * Generate an output event
   */
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event, long timeout,
      TimeUnit unit) throws IOException, InterruptedException;
   
}
