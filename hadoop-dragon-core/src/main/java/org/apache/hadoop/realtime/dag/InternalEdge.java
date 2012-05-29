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

import java.io.Serializable;

/**
 * An internal view of edges from a {@link DirectedAcyclicGraph}
 */
class InternalEdge<V> implements Serializable {
  private static final long serialVersionUID = 1917792937475311485L;

  V source;
  V target;

  InternalEdge(V source, V target) {
    this.source = source;
    this.target = target;
  }
  
  /**
   * Get the source vertex of this edge.
   * @return the source vertex of this edge.
   */
  public V getSource() {
    return source;
  }

  /**
   * Get the target vertex of this edge.
   * @return the target vertex of this edge.
   */
  public V getTarget() {
    return target;
  }

}