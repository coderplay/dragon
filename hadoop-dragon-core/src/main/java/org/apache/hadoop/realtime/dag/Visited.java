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

import java.util.ArrayList;
import java.util.List;

/**
 */
class Visited {
  private final List<Boolean> visited = new ArrayList<Boolean>();

  private Region affectedRegion;

  protected Visited(Region affectedRegion) {
    // Make sure visited is big enough
    int minSize = (affectedRegion.finish - affectedRegion.start) + 1;
    /* plus one because the region range is inclusive of both indices */

    while (visited.size() < minSize) {
      visited.add(Boolean.FALSE);
    }

    this.affectedRegion = affectedRegion;
  }
 
  public static Visited getInstance(Region affectedRegion) {
    return new Visited(affectedRegion);
  }

  public void setVisited(int index) {
    System.out.println("set visited: " + index );
    visited.set(translateIndex(index), Boolean.TRUE);
  }

  public boolean getVisited(int index) {
    Boolean result = null;

    result = visited.get(translateIndex(index));

    return result;
  }

  public void clearVisited(int index) throws UnsupportedOperationException {
    visited.set(translateIndex(index), Boolean.FALSE);
  }

  /**
   * We translate the topological index to an ArrayList index. We have to do
   * this because topological indices can be negative, and we want to do it
   * because we can make better use of space by only needing an ArrayList of
   * size |AR|.
   * 
   * @param unscaledIndex
   * 
   * @return the ArrayList index
   */
  private int translateIndex(int index) {
    return index - affectedRegion.start;
  }
}
