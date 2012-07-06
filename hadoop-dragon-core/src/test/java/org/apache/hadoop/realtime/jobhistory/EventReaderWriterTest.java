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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.realtime.jobhistory.event.JobCompletedEvent;
import org.apache.hadoop.realtime.jobhistory.event.JobInitedEvent;
import org.apache.hadoop.realtime.jobhistory.event.JobStartedEvent;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.util.DragonBuilderUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * class description goes here.
 *
 * @author xiaofeng_metis
 */
public class EventReaderWriterTest {

  FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

  ApplicationId appId = BuilderUtils.newApplicationId(200, 1);

  HistoryEvent event1 = new JobInitedEvent(BuilderUtils.newApplicationAttemptId(appId, 1));
  HistoryEvent event2 = new JobStartedEvent(DragonBuilderUtils.newJobId(appId, 1));
  HistoryEvent event3 = new JobCompletedEvent(DragonBuilderUtils.newJobId(appId, 1));

  ByteBuffer buffer = ByteBuffer.allocate(4096);

  @Test
  public void testWriteThenRead() throws IOException {
    buffer.clear();

    doAnswer(new Answer() {
      public Object answer(InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        buffer.put((byte[])args[0], (Integer)args[1], (Integer)args[2]);
        return null;
      }})
    .when(outputStream).write(any(byte[].class), anyInt(), anyInt());

    EventWriter writer = new EventWriter(outputStream);
    writer.write(event1);
    writer.write(event2);
    writer.write(event3);

    writer.flush();
    writer.close();

    verify(outputStream, times(2)).write(any(byte[].class), anyInt(), anyInt());
    verify(outputStream, times(1)).hflush();
    verify(outputStream, times(1)).close();

    EventReader reader = new EventReader(
        new ByteArrayInputStream(buffer.array()));

    assertTrue(reader.hasNext());
    HistoryEvent newEvent1 = reader.nextEvent();

    assertTrue(reader.hasNext());
    HistoryEvent newEvent2 = reader.nextEvent();

    assertTrue(reader.hasNext());
    HistoryEvent newEvent3 = reader.nextEvent();

    assertTrue(newEvent1 instanceof JobInitedEvent);
    assertTrue(newEvent2 instanceof JobStartedEvent);
    assertTrue(newEvent3 instanceof JobCompletedEvent);

    reader.close();
  }

}
