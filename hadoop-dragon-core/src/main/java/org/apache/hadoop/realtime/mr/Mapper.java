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
import org.apache.hadoop.realtime.event.Event;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * Called once at the beginning of the task.
   */
  protected void setup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Called once for each event in the input. Most applications should override
   * this, but the default is the identity function.
   */
  @SuppressWarnings("unchecked")
  protected void map(Event<KEYIN, VALUEIN> event,
      MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException,
      InterruptedException {
    context.emitEvent((Event<KEYOUT, VALUEOUT>) event);
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * 
   * @param context
   * @throws IOException
   */
  public void run(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    setup(context);
    try {
      while (true) {
        Event<KEYIN, VALUEIN> evt = context.pollEvent();
        if (evt != null)
          map(evt, context);
      }
    } catch (InterruptedException ie) {
      cleanup(context);
      throw ie;
    }
  }
}
