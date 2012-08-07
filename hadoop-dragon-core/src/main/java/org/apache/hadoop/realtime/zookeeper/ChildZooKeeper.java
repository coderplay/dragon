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
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * class description goes here.
 */
public class ChildZooKeeper implements Closeable {

  private final CuratorFramework zkClient;
  private final String zkRoot;
  private final String jobId;
  private String shufflePath;
  private volatile PathChildrenCache shuffleNodeCache;

  public ChildZooKeeper(final String serverList,
                            final String zkRoot,
                            final String jobId) throws IOException {
    this.zkClient = CuratorFrameworkFactory.newClient(
        serverList, new ExponentialBackoffRetry(300, 5));
    this.zkRoot = zkRoot;
    this.jobId = jobId;
    this.shufflePath = ZKPaths.makePath(
        ZKPaths.makePath(this.zkRoot, this.jobId), "shuffle");
  }

  public List<String> getShuffleNodeList() throws Exception {
    if (shuffleNodeCache == null) {
      synchronized (this) {
        if (shuffleNodeCache == null) {
          shuffleNodeCache = new PathChildrenCache(zkClient, shufflePath, true);
          shuffleNodeCache.start();
        }
      }
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    this.shuffleNodeCache.close();
    this.zkClient.close();
  }
}
