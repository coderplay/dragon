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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.io.Closeable;
import java.io.IOException;

/**
 * class description goes here.
 */
public class NMZooKeeper implements Closeable {

  private static Log LOG = LogFactory.getLog(NMZooKeeper.class);
  private final DragonZooKeeper dragonZK;

  public NMZooKeeper(
    final String serverList,
    final String zkRoot) throws IOException {
    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
        serverList, new ExponentialBackoffRetry(300, 5));
    this.dragonZK = new DragonZooKeeper(zkClient, zkRoot);
  }

  public void registerSelf(final NodeId nodeId) throws Exception {
    this.dragonZK.registerNodeManager(nodeId);
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(LOG, this.dragonZK);
  }
}
