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

package org.apache.hadoop.realtime.mr;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.realtime.child.Context;
import org.apache.hadoop.realtime.event.Event;

/** 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Called once at the start of the task.
   */
  protected void setup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation is
   * an identity function.
   */
  @SuppressWarnings("unchecked")
  protected abstract void reduce(Event<KEYIN, VALUEIN> event,
      ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException;

  /**
   * Called once at the end of the task.
   */
  protected void
      cleanup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
          throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Advanced application writers can use the
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to control
   * how the reduce task works.
   */
  public void run(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    setup(context);
    try {
      while (true) {
        reduce(context.pollEvent(), context);
      }
    } catch (InterruptedException ie) {
      cleanup(context);
      throw ie;
    }
  }
}
