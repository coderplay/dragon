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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * class description goes here.
 */
public class DragonZooKeeperTest {

  private TestingServer zkServer;
  private DragonZooKeeper dragonZK;
  private CuratorFramework zkClient;
  private boolean nodeDied = false;

  @Before
  public void setup() throws Exception {
    zkServer = new TestingServer(2222);
    zkClient = CuratorFrameworkFactory.newClient(
      zkServer.getConnectString(),
      new ExponentialBackoffRetry(300, 5));
    dragonZK = new DragonZooKeeper(zkClient, "/dragon");
  }

  @After
  public void tearDown() throws Exception {
    dragonZK.close();
    zkServer.stop();
  }

  private ApplicationId createTestAppId() {
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(458495849584L);
    appId.setId(89894985);
    return appId;
  }

  private NodeId createTestNodeId() {
    NodeId nodeId = Records.newRecord(NodeId.class);
    nodeId.setHost("ServerHost");
    nodeId.setPort(2345);
    return nodeId;
  }

  @Test
  public void testGetNodeManagerPath() throws Exception {
    NodeId nodeId = createTestNodeId();

    assertEquals(
      "/dragon/nodemanagers/ServerHost:2345",
      dragonZK.getNodeManagerPath(nodeId));
  }

  @Test
  public void testGetShufflePath() throws Exception {

    ApplicationId appId = createTestAppId();

    JobId jobId = JobId.newJobId(appId, 123456);

    assertEquals(
      "/dragon/job_458495849584_89894985_123456/shuffle",
      dragonZK.getShufflePath(jobId));
  }

  @Test
  public void testGetTaskPath() throws Exception {
    ApplicationId appId = createTestAppId();

    JobId jobId = JobId.newJobId(appId, 123456);
    TaskId taskId = TaskId.newTaskId(jobId, 34565, TaskType.MAP);

    assertEquals(
      "/dragon/job_458495849584_89894985_123456/shuffle/task_458495849584_89894985_123456_m_34565",
      dragonZK.getTaskPath(jobId, taskId));
  }

  @Test
  public void testRegisterNodeManager() throws Exception {
    NodeId nodeId = createTestNodeId();

    dragonZK.registerNodeManager(nodeId);

    Stat stat = zkClient.
      checkExists().
      forPath("/dragon/nodemanagers/ServerHost:2345");
    assertNotNull(stat);
  }

  @Test
  public void testCreateShufflePath() throws Exception {
    ApplicationId appId = createTestAppId();

    JobId jobId = JobId.newJobId(appId, 123456);

    dragonZK.createShufflePath(jobId);

    Stat stat = zkClient.
      checkExists().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle");
    assertNotNull(stat);
  }

  @Test
  public void testCreateShuffleNode() throws Exception {
    ApplicationId appId = createTestAppId();
    JobId jobId = JobId.newJobId(appId, 123456);

    DragonZooKeeper.NodeData node = new DragonZooKeeper.NodeData();
    node.nodeId = createTestNodeId();
    node.taskId = TaskId.newTaskId(jobId, 34565, TaskType.MAP);

    dragonZK.createShufflePath(jobId);
    dragonZK.createShuffleNode(jobId, newArrayList(node));

    Stat stat = zkClient.
      checkExists().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle/task_458495849584_89894985_123456_m_34565");
    assertNotNull(stat);

    byte[] data = zkClient.
      getData().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle/task_458495849584_89894985_123456_m_34565");
    assertEquals("ServerHost:2345", new String(data));
  }

  @Test
  public void testUpdateShuffleNode() throws Exception {
    JobId jobId = JobId.newJobId(createTestAppId(), 123456);

    DragonZooKeeper.NodeData node = new DragonZooKeeper.NodeData();
    node.nodeId = createTestNodeId();
    node.taskId = TaskId.newTaskId(jobId, 34565, TaskType.MAP);

    dragonZK.createShufflePath(jobId);
    dragonZK.createShuffleNode(jobId, newArrayList(node));

    node.nodeId.setHost("NewServerHost");
    dragonZK.updateShuffleNode(jobId, newArrayList(node));

    Stat stat = zkClient.
      checkExists().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle/task_458495849584_89894985_123456_m_34565");
    assertNotNull(stat);

    byte[] data = zkClient.
      getData().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle/task_458495849584_89894985_123456_m_34565");
    assertEquals("NewServerHost:2345", new String(data));
  }

  @Test
  public void testGetShuffleNodeByTaskId() throws Exception {
    JobId jobId = JobId.newJobId(createTestAppId(), 123456);
    TaskId taskId = TaskId.newTaskId(jobId, 34565, TaskType.MAP);
    DragonZooKeeper.NodeData node = new DragonZooKeeper.NodeData();
    node.nodeId = createTestNodeId();
    node.taskId = taskId;

    if (zkClient.
      checkExists().
      forPath("/dragon/job_458495849584_89894985_123456/shuffle") == null) {
      dragonZK.createShufflePath(jobId);
    }

    PathChildrenCache shuffleNodeCache = new PathChildrenCache(
      zkClient,
      dragonZK.getShufflePath(jobId),
      true);
    dragonZK.setShuffleNodeCache(shuffleNodeCache);

    dragonZK.createShuffleNode(jobId, newArrayList(node));
    Thread.sleep(1000);

    NodeId nodeId = dragonZK.getShuffleNodeByTaskId(jobId, taskId);
    assertNotNull(nodeId);
    assertEquals(node.nodeId, nodeId);
  }

  @Test
  public void testGetAvailableNodes() throws Exception {
    NodeId nodeId = createTestNodeId();

    dragonZK.registerNodeManager(nodeId);
    List<NodeId> nodeIdList = dragonZK.getAvailableNodes();
    assertEquals(nodeId, nodeIdList.get(0));
  }

  @Test
  public void testWatchNodeManager() throws Exception {
    NodeId nodeId = createTestNodeId();
    dragonZK.registerNodeManager(nodeId);
    nodeDied = false;

    dragonZK.watchNodeManager(nodeId, new CuratorWatcher() {
      @Override
      public void process(WatchedEvent event) throws Exception {
        nodeDied = true;
      }
    });

    zkClient.delete().forPath("/dragon/nodemanagers/ServerHost:2345");
    Thread.sleep(1000);
    assertTrue(nodeDied);
  }
}
