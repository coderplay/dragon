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
package org.apache.hadoop.realtime;

import java.io.Serializable;

import org.apache.hadoop.realtime.event.EventProcessor;
import org.apache.hadoop.realtime.event.EventProducer;

/**
 * 
 */
public class DragonVertex implements Serializable {

  private static final long serialVersionUID = -8959502704094556166L;

  DragonVertex() {
  };

  DragonVertex(Builder builder) {

  }
  
  @Override
  public int hashCode() {
    // TODO: implements it
    return -1;
  }

  public static final class Builder {
    String label;

    public Builder(String label) {
      this.label = label;
    }

    public Builder jars(String[] jars) {
      return this;
    }

    public Builder archieves(String[] archieves) {
      return this;
    }

    public Builder files(String[] files) {
      return this;
    }

    public Builder producer(Class<? extends EventProducer> clazz) {
      return this;
    }

    public Builder processor(Class<? extends EventProcessor> clazz) {
      return this;
    }

    public Builder tasks(int num) {
      return this;
    }

    public Builder childOptions(String options) {
      return this;
    }

    public DragonVertex build() {
      return new DragonVertex(this);
    }
  }
}