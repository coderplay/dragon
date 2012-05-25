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
 * An {@link EdgeFactory} for producing edges by using a class as a factory.
 */
public class EdgeFactory<V, E> implements Serializable {
  private static final long serialVersionUID = -7890706652865009651L;

  private final Class<? extends E> edgeClass;

  public EdgeFactory(Class<? extends E> edgeClass) {
    this.edgeClass = edgeClass;
  }

  /**
   * @see EdgeFactory#createEdge(Object, Object)
   */
  public E createEdge(V source, V target) {
    try {
      return edgeClass.newInstance();
    } catch (Exception ex) {
      throw new RuntimeException("Edge factory failed", ex);
    }
  }
}