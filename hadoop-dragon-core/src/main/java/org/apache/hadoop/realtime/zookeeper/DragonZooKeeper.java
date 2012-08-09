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
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.netflix.curator.utils.ZKPaths.makePath;

/**
 * class description goes here.
 */
public class DragonZooKeeper implements Closeable {
  public static final String SHUFFLE_ZK_PATH = "shuffle";
  public static final String NODE_MANAGERS_ZK_PATH = "nodemanagers";

  private static final Log LOG = LogFactory.getLog(DragonZKService.class);
  public static final Splitter SPLITTER = Splitter.on(":");

  private CuratorFramework zkClient;
  private String zkRoot;
  private String nodeManagersPath;

  private PathChildrenCache nodeManagersCache;
  private Map<JobId, PathChildrenCache> shuffleNodeCacheMap;

  public DragonZooKeeper(CuratorFramework zkClient, String zkRoot) {
    this.zkClient = zkClient;
    this.zkRoot = zkRoot;
    this.nodeManagersPath = makePath(zkRoot, NODE_MANAGERS_ZK_PATH);
  }

  public CuratorFramework getZkClient() {
    return this.zkClient;
  }

  public String getNodeManagersPath() {
    return nodeManagersPath;
  }

  public String getShufflePath(JobId jobId) {
    return makePath(
      makePath(this.zkRoot, jobId.toString()), SHUFFLE_ZK_PATH);
  }

  public void setNodeManagersCache(
    final PathChildrenCache nodeManagersCache) throws Exception {
    this.nodeManagersCache = nodeManagersCache;
    this.nodeManagersCache.start();
  }

  public void addShuffleNodeCache(final JobId jobId,
                                  final PathChildrenCache shuffleNodeCache)
    throws Exception {

    if (this.shuffleNodeCacheMap == null) {
      this.shuffleNodeCacheMap = new HashMap<JobId, PathChildrenCache>();
    }

    this.shuffleNodeCacheMap.put(jobId, shuffleNodeCache);

    shuffleNodeCache.start();
  }

  public void registerNodeManager(final NodeId nodeId) throws Exception {
    final String nodePath = makePath(this.nodeManagersPath, nodeId.toString());
    this.zkClient.create().creatingParentsIfNeeded().
      withMode(CreateMode.EPHEMERAL).forPath(nodePath);
  }

  public NodeId getShuffleNodeByTaskId(final JobId jobId,
                                       final TaskId  taskId) {
    final String taskPath = makePath(getShufflePath(jobId), taskId.toString());
    final PathChildrenCache shuffleNodeCache = shuffleNodeCacheMap.get(jobId);
    final ChildData taskData = shuffleNodeCache.getCurrentData(taskPath);
    final Iterator<String> nodeIdIt =
      SPLITTER.split(new String(taskData.getData())).iterator();

    final NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost(nodeIdIt.next());
    nodeId.setPort(Integer.parseInt(nodeIdIt.next()));

    return nodeId;
  }

  public void registerTasks(final JobId jobId,
                            final List<NodeData> nodes) {
    final String shufflePath = getShufflePath(jobId);

    try {
      for (NodeData nodeData : nodes) {
        byte[] nodeBytes = nodeData.nodeId.toString().getBytes();

        for (TaskId taskId : nodeData.taskIds) {
          final String taskPath = makePath(shufflePath, taskId.toString());
          this.zkClient.create().
            creatingParentsIfNeeded().forPath(taskPath, nodeBytes);
        }
      }
    } catch (Exception ex) {
      LOG.error("nodes register failure ", ex);
    }
  }

  public void renewNode(final JobId jobId,
                        final NodeId nodeId,
                        final TaskId taskId)  {
    final String taskPath = makePath(
      makePath(
        makePath(this.zkRoot, jobId.toString()),
        SHUFFLE_ZK_PATH), taskId.toString()
    );

    try {
      this.zkClient.setData().forPath(taskPath, nodeId.toString().getBytes());
    } catch (Exception ex) {
      LOG.error("node renew failure ", ex);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(LOG, nodeManagersCache);

    for (PathChildrenCache shuffleNodeCache : shuffleNodeCacheMap.values()) {
      IOUtils.cleanup(LOG, shuffleNodeCache);
    }
  }

  public static class NodeData {
    public NodeId nodeId;
    public List<TaskId> taskIds;
  }

  public class NMPathChildrenCacheListener
    implements PathChildrenCacheListener {

    private final AppContext appContext;

    public NMPathChildrenCacheListener(final AppContext appContext) {
      this.appContext = appContext;
    }

    @Override
    public void childEvent(
      final CuratorFramework client,
      final PathChildrenCacheEvent event) throws Exception {

      if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
        for (Job job : appContext.getAllJobs().values()) {

          final PathChildrenCache shuffleNodeCache =
            shuffleNodeCacheMap.get(job.getID());
          final String nodeDied = event.getData().getPath();

          byte[] nextNode = null;
          for (ChildData taskData : shuffleNodeCache.getCurrentData()) {
            byte[] node = taskData.getData();
            if (!nodeDied.equals(new String(node))) {
              nextNode = node;
              break;
            }
          }

          if (nextNode == null) {
            final String err =
              "all node managers are died in the application " + job.getID();
            LOG.error(err);
            throw new IllegalStateException(err);
          }

          for (ChildData taskData : shuffleNodeCache.getCurrentData()) {
            if (nodeDied.equals(new String(taskData.getData()))) {
              zkClient.setData().forPath(taskData.getPath(), nextNode);
            }
          }
        }
      }
    }
  }
}
