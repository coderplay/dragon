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

import com.google.common.base.Splitter;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.netflix.curator.utils.ZKPaths.getNodeFromPath;
import static com.netflix.curator.utils.ZKPaths.makePath;

/**
 * common zookeeper code for dragon
 */
public class DragonZooKeeper implements Closeable {
  public static final String SHUFFLE_ZK_PATH = "shuffle";
  public static final String NODE_MANAGERS_ZK_PATH = "nodemanagers";

  private static final Log LOG = LogFactory.getLog(DragonZKService.class);
  public static final Splitter SPLITTER = Splitter.on(":");

  private final CuratorFramework zkClient;
  private final String zkRoot;
  private final String nodeManagersPath;

  private PathChildrenCache shuffleNodeCache;

  public DragonZooKeeper(final CuratorFramework zkClient, final String zkRoot) {
    this.zkClient = zkClient;
    this.zkRoot = zkRoot;
    this.nodeManagersPath = makePath(zkRoot, NODE_MANAGERS_ZK_PATH);
    zkClient.start();
  }

  public String getNodeManagerPath(final NodeId nodeId) {
    return makePath(nodeManagersPath, nodeId.toString());
  }

  public String getShufflePath(final JobId jobId) {
    return makePath(
      makePath(this.zkRoot, jobId.toString()), SHUFFLE_ZK_PATH);
  }

  public String getTaskPath(final JobId jobId, final TaskId taskId) {
    return makePath(getShufflePath(jobId), taskId.toString());
  }

  public void setShuffleNodeCache(final PathChildrenCache shuffleNodeCache)
    throws Exception {
    this.shuffleNodeCache = shuffleNodeCache;
    this.shuffleNodeCache.start();
  }

  public void registerNodeManager(final NodeId nodeId) throws Exception {
    final String nodePath = getNodeManagerPath(nodeId);
    this.zkClient.create().creatingParentsIfNeeded().
      withMode(CreateMode.EPHEMERAL).forPath(nodePath);
  }

  public void createShufflePath(final JobId jobId) throws Exception {
    final String shufflePath = getShufflePath(jobId);
    this.zkClient.create().creatingParentsIfNeeded().forPath(shufflePath);
  }

  public NodeId getShuffleNodeByTaskId(final JobId jobId,
                                       final TaskId  taskId) {
    final String taskPath = getTaskPath(jobId, taskId);
    final ChildData taskData = shuffleNodeCache.getCurrentData(taskPath);
    if (taskData == null) {
      return null;
    }

    return toNodeID(new String(taskData.getData()));
  }

  public void createShuffleNode(final JobId jobId,
                                final List<NodeData> nodeList)
    throws Exception {

    for (NodeData nodeData : nodeList) {
      final String taskPath = getTaskPath(jobId, nodeData.taskId);
      zkClient.
        create().
        forPath(taskPath, nodeData.nodeId.toString().getBytes());
    }

  }

  public void updateShuffleNode(final JobId jobId,
                                final List<NodeData> nodeList)
    throws Exception {

    for (NodeData nodeData : nodeList) {
      String taskPath = getTaskPath(jobId, nodeData.taskId);
      this.zkClient.setData().forPath(taskPath,
        nodeData.nodeId.toString().getBytes());
    }
  }

  public List<NodeId> getAvailableNodes() throws Exception {
    List<String> children = zkClient.getChildren().forPath(nodeManagersPath);

    List<NodeId> nodeIds = newArrayList();
    for (String child : children) {
      nodeIds.add(toNodeID(child));
    }

    return nodeIds;
  }

  public void watchNodeManager(
    final NodeId nodeId,
    final CuratorWatcher nmWatcher) throws Exception {

    String nodePath = getNodeManagerPath(nodeId);
    zkClient.
      checkExists().
      usingWatcher(nmWatcher).
      inBackground().
      forPath(nodePath);
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(LOG, shuffleNodeCache);
  }

  public static class NodeData {
    public NodeId nodeId;
    public TaskId taskId;
  }

  public static NodeId toNodeID(String data) {
    final Iterator<String> nodeIdIt = SPLITTER.split(data).iterator();

    final NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(nodeIdIt.next());
    nodeId.setPort(Integer.parseInt(nodeIdIt.next()));

    return nodeId;
  }


}
