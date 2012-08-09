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

import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.yarn.api.records.NodeId;
import static org.apache.hadoop.realtime.zookeeper.DragonZooKeeper.NodeData;

/**
 * class description goes here.
 */
public class NodeRenewEvent extends ZKEvent {

  private final NodeId nodeId;
  private final TaskId taskId;

  public NodeRenewEvent(final JobId jobId,
                        final NodeId nodeId,
                        final TaskId taskId) {
    super(jobId, ZKEventType.NODE_RENEW);

    this.nodeId = nodeId;
    this.taskId = taskId;
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public TaskId getTaskId() {
    return taskId;
  }
}
