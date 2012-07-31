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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.counter.TaskCounter;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.mr.MapContext;
import org.apache.hadoop.realtime.mr.MapContextImpl;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.Counter;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.util.ReflectionUtils;

/**
 */
final class MapChildExecutor extends ChildExecutor {

  private Counter inputCounter;
  private Counter outputCounter;

  public MapChildExecutor(ChildExecutionContext context) {
    super(context);
    this.inputCounter = counters.getCounter(TaskCounter.MAP_INPUT_RECORDS);
    this.outputCounter = counters.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
  }

  private static class DirectOutputCollector<K, V> implements
      EventEmitter<K, V> {
    private final EventEmitter<K, V> out;
    private final List<Statistics> fsStats;

    // counters

    @SuppressWarnings("unchecked")
    DirectOutputCollector(DragonConfiguration conf) throws IOException,
        ClassNotFoundException, InterruptedException {
      this.out = null;
      String name = conf.get(DragonJobConfig.JOB_OUTPUT_DIR);
      this.fsStats =
          getFsStatistics(name == null ? null : new Path(name), conf);
    }

    @Override
    public void close() throws IOException {
      if (out != null) {
        out.close();
      }
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null)
        return 0;
      long bytesWritten = 0;
      for (Statistics stat : stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }

    @Override
    public boolean emitEvent(Event<K, V> event) throws IOException,
        InterruptedException {
      return out.emitEvent(event);
    }

    @Override
    public boolean emitEvent(Event<K, V> event, long timeout, TimeUnit unit)
        throws IOException, InterruptedException {
      return out.emitEvent(event, timeout, unit);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <KEYIN, VALUEIN, KEYOUT, VALUEOUT> void execute(Configuration conf,
      Class<KEYIN> keyInClass, Class<VALUEIN> valueInClass,
      Class<KEYOUT> keyOutClass, Class<VALUEOUT> valueOutClass)
      throws IOException, InterruptedException {

    // make a mapper
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper =
        (Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) ReflectionUtils.newInstance(
            conf.getClass(DragonJobConfig.JOB_MAP_CLASS, Mapper.class), conf);
    // fetch the input sub-dirs

    // make the event producer
    EventProducer<KEYIN, VALUEIN> input = null;
    EventEmitter<KEYOUT, VALUEOUT> output = null;
    // if reduce number = 0 , then output = DirectOutputCollector
    // else output = OutputCollector

    // make a map context
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext =
        new MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(conf, context,
            inputCounter, outputCounter, input, output);

    // initialize the event producer
    // run the mapper
    mapper.run(mapContext);
    // close the event producer

  }

}
