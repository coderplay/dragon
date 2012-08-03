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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.mr.ReduceContext;
import org.apache.hadoop.realtime.mr.ReduceContextImpl;
import org.apache.hadoop.realtime.mr.Reducer;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 */
public class ReduceChildExecutor extends ChildExecutor {


  public ReduceChildExecutor(ChildExecutionContext context) {
    super(context);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void execute(Configuration conf,
      Class<KEYIN> keyInClass, Class<VALUEIN> valueInClass,
      Class<KEYOUT> keyOutClass, Class<VALUEOUT> valueOutClass,TaskReporter reporter)
      throws IOException, InterruptedException {

    // make a reducer
    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer =
        (Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) ReflectionUtils
            .newInstance(
                conf.getClass(DragonJobConfig.JOB_REDUCE_CLASS, Reducer.class),
                conf);
    // fetch the input sub-dirs

    // make the event producer
    EventProducer<KEYIN, VALUEIN> input = null;
    EventEmitter<KEYOUT, VALUEOUT> output = null;
    
    // if reduce number = 0 , then output = DirectOutputCollector
    // else output = OutputCollector

    // make a map context
    ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext =
        new ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, context,
            reporter, input, output);

    // initialize the event producer
    // run the mapper
    reducer.run(reduceContext);
    // close the event producer

  }
}
