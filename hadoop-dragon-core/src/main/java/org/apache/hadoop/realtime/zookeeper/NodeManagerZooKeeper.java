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
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * class description goes here.
 */
public class NodeManagerZooKeeper implements Closeable {

  private final CuratorFramework zkClient;
  private final String zkRoot;
  private final String nodeManagersPath;

  private static Log LOG = LogFactory.getLog(NodeManagerZooKeeper.class);

  public NodeManagerZooKeeper(
      final String serverList,
      final String zkRoot) throws IOException {
    this.zkClient = CuratorFrameworkFactory.newClient(
        serverList, new ExponentialBackoffRetry(300, 5));

    this.zkRoot = zkRoot;
    this.nodeManagersPath = ZKPaths.makePath(zkRoot, "nodemanagers");
  }

  public void register(final String nodeId) throws Exception {
    final String nodePath = ZKPaths.makePath(this.nodeManagersPath, nodeId);
    this.zkClient.create().creatingParentsIfNeeded().forPath(nodePath);
  }

  @Override
  public void close() throws IOException {
    this.zkClient.close();
  }
}
