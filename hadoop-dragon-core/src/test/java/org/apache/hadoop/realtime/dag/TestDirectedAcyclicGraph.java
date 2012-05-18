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
package org.apache.hadoop.realtime.dag;

import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestDirectedAcyclicGraph extends TestCase {

  public void testAddVertex() {
    // vertex type: String, edge type: String
    String vertex1 = "new vertex1";
    String vertex2 = "new vertex2";
    DirectedAcyclicGraph<String, Object> dag =
        new DirectedAcyclicGraph<String, Object>(Object.class);
    dag.addVertex(vertex1);
    assertTrue(dag.containsVertex(vertex1));
    assertFalse(dag.containsVertex(vertex2));
  }

  public void testCycleDetection() {
    // vertex type: String, edge type: String
    String vertex1 = "new vertex1";
    String vertex2 = "new vertex2";
    String vertex3 = "new vertex3";
    DirectedAcyclicGraph<String, Object> dag =
        new DirectedAcyclicGraph<String, Object>(Object.class);
    dag.addVertex(vertex1);
    dag.addVertex(vertex2);
    dag.addVertex(vertex3);
    boolean dagRejectedEdge = false;
    try {
      dag.addEdge(vertex1, vertex2);
      dag.addEdge(vertex2, vertex3);
      dag.addEdge(vertex3, vertex1);
    } catch (CycleFoundException e) {
      dagRejectedEdge = true;
    }

    assertTrue(dagRejectedEdge);
  }
  
  public void textIteration() throws Exception {
    String vertex1 = "new vertex1";
    String vertex2 = "new vertex2";
    String vertex3 = "new vertex3";
    String vertex4 = "new vertex4";
    DirectedAcyclicGraph<String, Object> dag =
        new DirectedAcyclicGraph<String, Object>(Object.class);
    // a diamond topology
    dag.addEdge(vertex1, vertex2);
    dag.addEdge(vertex1, vertex3);
    dag.addEdge(vertex2, vertex4);
    dag.addEdge(vertex3, vertex4);

    Iterator<String> iter = dag.iterator();
    assertTrue(iter.hasNext());
    assertEquals(iter.next(), "new vertex1");
    assertEquals(iter.next(), "new vertex2");
    assertEquals(iter.next(), "new vertex3");
    assertEquals(iter.next(), "new vertex4");
    assertFalse(iter.hasNext());
  }

}
