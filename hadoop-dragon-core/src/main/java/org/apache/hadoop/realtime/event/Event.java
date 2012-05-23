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
package org.apache.hadoop.realtime.event;

import java.io.Serializable;

/**
 * Event is the basic processing unit of dragon. 
 */
public interface Event<KEY, VALUE> extends Serializable {

  /**
   * Every {@link Event} should provide a <code>offset</code> 
   * for helping checkpointing.
   * @return the offset of this {@link Event}
   */
  public long offset();

  /**
   * The key of this {@link Event} 
   * @return the key of this {@link Event} 
   */
  public KEY key();
  
  /**
   * The value of this {@link Event} 
   * @return the value of this {@link Event} 
   */
  public VALUE value();

}
