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
package org.apache.hadoop.realtime.zookeeper;

import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * when node manager is died, DragonZKService will fire this event
 */
public class NodeManagerDiedEvent extends JobEvent {
  private final NodeId diedNode;
  private final List<NodeId> availableNodes;

  public NodeManagerDiedEvent(final JobId jobId,
                              final NodeId diedNode,
                              final List<NodeId> availableNodes) {
    super(jobId, JobEventType.NODE_MANAGER_DIED);

    this.diedNode = diedNode;
    this.availableNodes = availableNodes;
  }

  public NodeId getDiedNode() {
    return diedNode;
  }

  public List<NodeId> getAvailableNodes() {
    return availableNodes;
  }
}
