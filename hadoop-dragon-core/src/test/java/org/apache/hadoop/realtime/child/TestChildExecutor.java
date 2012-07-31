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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.event.EventProducer;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Before;
import org.junit.Test;

public class TestChildExecutor {

  private final static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  ChildExecutor executor;
  Configuration conf;
  ChildServiceDelegate delegate;

  @Before
  public void init() {
    ChildExecutionContext context =
        recordFactory.newRecordInstance(ChildExecutionContext.class);
    TaskAttemptId attemptId =
        TaskAttemptId.parseTaskAttemptId("attempt_1343700122251_1_1_MAP_2_3");
    context.setTaskAttemptId(attemptId);
    context.setPartition(0);
    context.setUser("Test");
    context.setTaskType(TaskType.MAP);
    executor = ChildExecutorFactory.newExecutor(context);
    conf = new Configuration();

    conf.setClass(DragonJobConfig.JOB_MAP_CLASS, MockMapper.class, Mapper.class);
    conf.setClass(DragonJobConfig.JOB_EVENT_PRODUCER_CLASS,
        MockEventProducer.class, EventProducer.class);
    conf.setClass(DragonJobConfig.JOB_EVENT_EMITTER_CLASS,
        MockEventEmitter.class, EventEmitter.class);
    conf.setClass(DragonJobConfig.JOB_MAP_INPUT_KEY_CLASS, Object.class,
        Object.class);
    conf.setClass(DragonJobConfig.JOB_MAP_INPUT_VALUE_CLASS, Object.class,
        Object.class);
    conf.setClass(DragonJobConfig.JOB_MAP_OUTPUT_KEY_CLASS, Object.class,
        Object.class);
    conf.setClass(DragonJobConfig.JOB_MAP_OUTPUT_VALUE_CLASS, Object.class,
        Object.class);

    delegate =
        new MockChildServiceDelegate(conf, "job_1343700122251_1_1",
            NetUtils.createSocketAddr("localhost:3432"));
  }

  @Test
  public void test() {
    try {
      executor.execute(conf, delegate);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
