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
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.mr.MapContext;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 */
public class ReduceChildExecutor extends ChildExecutor {

  @Override
  @SuppressWarnings("unchecked")
  protected <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void execute(
      final Configuration conf, final DragonChildProtocol proxy,
      final ChildExecutionContext context) throws IOException,
      InterruptedException {
    super.execute(conf, proxy, context);

    // make a mapper
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper =
        (Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) ReflectionUtils.newInstance(
            conf.getClass(DragonJobConfig.JOB_MAP_CLASS, Mapper.class), conf);
    // fetch the input sub-dirs

    // make the event producer
    EventProducer<KEYIN, VALUEIN> input = null;

    // if reduce number = 0 , then output = DirectOutputCollector
    // else output = OutputCollector

    // make a map context
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext = null;

    // initialize the event producer
    // run the mapper
    mapper.run(mapContext);
    // close the event producer

  }
}
