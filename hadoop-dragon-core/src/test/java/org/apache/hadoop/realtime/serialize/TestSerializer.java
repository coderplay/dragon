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
package org.apache.hadoop.realtime.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.realtime.DragonEdge;
import org.apache.hadoop.realtime.DragonJobGraph;
import org.apache.hadoop.realtime.DragonVertex;
import org.apache.hadoop.realtime.dag.CycleFoundException;
import org.apache.hadoop.realtime.event.EventProcessor;
import org.apache.hadoop.realtime.event.EventProducer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;

/**
 */
public class TestSerializer {

  private DragonJobGraph graph = null;

  @Before
  public void setUp() throws CycleFoundException {
  }

  @Test
  public void serdeDagbyHessian() throws CycleFoundException, IOException {
    DragonVertex source =
        new DragonVertex.Builder("source").producer(EventProducer.class)
            .processor(EventProcessor.class).tasks(10).build();
    DragonVertex m1 =
        new DragonVertex.Builder("intermediate1")
            .processor(EventProcessor.class).addFile("file.txt")
            .addFile("dict.dat").addArchive("archive.zip").tasks(10).build();
    DragonVertex m2 =
        new DragonVertex.Builder("intermediate2")
            .processor(EventProcessor.class).addFile("aux").tasks(10).build();
    DragonVertex dest =
        new DragonVertex.Builder("dest").processor(EventProcessor.class)
            .tasks(10).build();
    graph = new DragonJobGraph();
    // check if the graph is cyclic when adding edge
    graph.addEdge(source, m1);
    graph.addEdge(source, m2);
    graph.addEdge(m1, dest);
    graph.addEdge(m2, dest);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    HessianSerializer<DragonJobGraph> serializer =
        new HessianSerializer<DragonJobGraph>();
    serializer.serialize(out, graph);
    byte[] bytes = out.toByteArray();

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DragonJobGraph dag = serializer.deserialize(in);

    Iterator<DragonVertex> iter1 = graph.iterator();
    Iterator<DragonVertex> iter2 = dag.iterator();

    while (iter1.hasNext()) {
      assertTrue(iter2.hasNext());
      DragonVertex next1 = iter1.next();
      DragonVertex next2 = iter2.next();
      assertEquals(next1.getLabel(), next2.getLabel());
    }

    assertFalse(iter2.hasNext());
  }
  
  @Test
  public void testNull() throws IOException {
    Object o = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    HessianSerializer<Object> serializer =
        new HessianSerializer<Object>();
    serializer.serialize(out, o);
    byte[] bytes = out.toByteArray();
    
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    Object result = serializer.deserialize(in);
    assertNull(result);
  }
}
