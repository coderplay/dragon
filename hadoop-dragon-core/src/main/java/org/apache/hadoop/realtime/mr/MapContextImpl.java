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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.app.counter.TaskCounter;
import org.apache.hadoop.realtime.child.ChildExecutor.TaskReporter;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counter;
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

  private static final Log LOG = LogFactory.getLog(MapContextImpl.class);

  private final ChildExecutionContext context;
  private final DragonConfiguration conf;

  private final TaskAttemptId attemptId;

  private KEYIN key; // current key
  private VALUEIN value;
  
  private TaskReporter reporter;

  private EventProducer<KEYIN, VALUEIN> producer;
  private EventEmitter<KEYOUT, VALUEOUT> emitter;

  public MapContextImpl(Configuration conf, ChildExecutionContext context,
      TaskReporter reporter,
      EventProducer<KEYIN, VALUEIN> producer,
      EventEmitter<KEYOUT, VALUEOUT> emitter) throws IOException {
    this.context = context;
    this.conf = new DragonConfiguration(conf);

    this.producer = producer;
    this.emitter = emitter;

    this.reporter = reporter;
    this.attemptId = context.getTaskAttemptId();
  }

  @Override
  public DragonConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    return attemptId;
  }

  @Override
  public int getPartition() {
    return context.getPartition();
  }

  @Override
  public String getUser() {
    return context.getUser();
  }

  @Override
  public TaskType getTaskType() {
    return TaskType.MAP;
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent() throws IOException,
      InterruptedException {
    reporter.incrCounter(TaskCounter.MAP_INPUT_RECORDS, 1);
    return producer.pollEvent();
  }

  @Override
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException { 
    reporter.incrCounter(TaskCounter.MAP_INPUT_RECORDS, 1);
    return producer.pollEvent(timeout, unit);
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event) throws IOException,
      InterruptedException {
    emitter.emitEvent(event);
    reporter.incrCounter(TaskCounter.MAP_OUTPUT_RECORDS, 1);
    return true;
  }

  @Override
  public boolean emitEvent(Event<KEYOUT, VALUEOUT> event, long timeout,
      TimeUnit unit) throws IOException, InterruptedException {
    emitter.emitEvent(event, timeout, unit);
    reporter.incrCounter(TaskCounter.MAP_OUTPUT_RECORDS, 1);
    return true;
  }

}
