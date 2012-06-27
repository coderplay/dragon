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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.realtime.dag.DirectedAcyclicGraph;
import org.apache.hadoop.realtime.event.EventProcessor;
import org.apache.hadoop.realtime.event.EventProducer;

/**
 * {@link DragonVertex}s is the basic component of a
 * {@link DirectedAcyclicGraph} for describing the topology of a {@link DragonJob}.
 * A {@link DirectedAcyclicGraph} for dragon jobs is formed by a collection of 
 * {@link DragonVertex}s and directed {@link DragonEdge}s.
 * 
 * @see DragonJob
 * @see DirectedAcyclicGraph
 * @see DragonEdge
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DragonVertex implements Serializable {

  private static final long serialVersionUID = -8959502704094556166L;
  private String label;
  private int tasks;
  private List<String> files;
  private List<String> archives;
  private String childOpts;

  DragonVertex() {
  };

  DragonVertex(Builder builder) {
    label = builder.label;
    files = builder.files;
    archives = builder.archives;
    childOpts = builder.childOpts;
    tasks = (builder.tasks > 0) ? builder.tasks : 1;
  }
  
  public String getLabel() {
    return label;
  }
  
  public List<String> getFiles() {
    return files;
  }
  
  public List<String> getArchives() {
    return archives;
  }
  
  String getChildOptins() {
    return childOpts;
  }
  
  public int getTasks() {
    return tasks;
  }

  public static final class Builder {
    String label;
    int tasks;
    List<String> jars;
    List<String> files;
    List<String> archives;
    String childOpts;

    public Builder(final String label) {
      this.label = label;
      jars = new ArrayList<String>();
      files = new ArrayList<String>();
      archives = new ArrayList<String>();
    }

    public Builder addArchive(final String archive){
      archives.add(archive);
      return this;
    }

    public Builder addFile(final String file) {
      files.add(file);
      return this;
    }

    public Builder producer(final Class<? extends EventProducer> clazz) {
      return this;
    }

    public Builder processor(final Class<? extends EventProcessor> clazz) {
      return this;
    }

    public Builder tasks(final int num) {
      this.tasks = num;
      return this;
    }

    public Builder childOptions(final String options) {
      childOpts = options;
      return this;
    }

    public DragonVertex build() {
      return new DragonVertex(this);
    }
  }
}