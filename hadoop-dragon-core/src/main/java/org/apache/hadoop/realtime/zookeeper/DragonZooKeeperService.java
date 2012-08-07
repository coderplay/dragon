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
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * class description goes here.
 */
public class DragonZooKeeperService extends AbstractService
    implements EventHandler<ZKEvent> {

  private CuratorFramework zkClient;
  private String zkRoot;
  private String nodeManagersPath;
  private String shufflePath;

  private PathChildrenCache nodeManagersCache;

  protected final BlockingQueue<ZKEvent> eventQueue =
      new LinkedBlockingQueue<ZKEvent>();

  private static final Log LOG = LogFactory.getLog(DragonZooKeeperService.class);
  private Thread eventHandlingThread;
  private volatile boolean stopped;

  private final Object lock = new Object();

  private final AppContext context;

  public DragonZooKeeperService(AppContext context) {
    super("DragonZooKeeperService");

    this.context = context;
  }

  @Override
  public synchronized void init(Configuration config) {
    final String serverList = config.get(DragonConfiguration.ZK_SERVER_LIST);
    try {
      this.zkClient = CuratorFrameworkFactory.newClient(
          serverList, new ExponentialBackoffRetry(300, 5));
    } catch (IOException e) {
      throw new IllegalStateException("init app master zookeeper failure", e);
    }

    this.zkRoot =  config.get(DragonConfiguration.ZK_ROOT);
    this.nodeManagersPath = ZKPaths.makePath(zkRoot, "nodemanagers");

    this.nodeManagersCache =
        new PathChildrenCache(zkClient, nodeManagersPath, false);

    this.nodeManagersCache.getListenable().addListener(
        new NMPathChildrenCacheListener());

    super.init(config);
  }

  @Override
  public synchronized void start() {
    try {
      this.nodeManagersCache.start();
    } catch (Exception e) {
      throw new IllegalStateException(
          "init app master zookeeper start node managers cache failure", e);
    }

    eventHandlingThread = new Thread(new Runnable() {
      @Override
      public void run() {
        ZKEvent event = null;
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.info("EventQueue take interrupted. Returning");
            return;
          }

          // If an event has been removed from the queue. Handle it.
          // The rest of the queue is handled via stop()
          // Clear the interrupt status if it's set before calling handleEvent
          // and set it if it was set before calling handleEvent.
          // Interrupts received from other threads during handleEvent cannot be
          // dealth with - Shell.runCommand() ignores them.
          synchronized (lock) {
            boolean isInterrupted = Thread.interrupted();
            handleEvent(event);
            if (isInterrupted) {
              Thread.currentThread().interrupt();
            }
          }
        }
      }
    });
    eventHandlingThread.start();
    super.start();
  }

  @Override
  public void handle(ZKEvent event) {
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping DragonZooKeeperService. "
        + "Size of the outstanding queue size is " + eventQueue.size());
    stopped = true;
    //do not interrupt while event handling is in progress
    synchronized(lock) {
      if (eventHandlingThread != null)
        eventHandlingThread.interrupt();
    }

    try {
      if (eventHandlingThread != null)
        eventHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info("Interruped Exception while stopping", ie);
    }

    //write all the events remaining in queue
    Iterator<ZKEvent> it = eventQueue.iterator();
    while(it.hasNext()) {
      ZKEvent ev = it.next();
      LOG.info("In stop, handling event " + ev.getType());
      handleEvent(ev);
    }

    IOUtils.cleanup(LOG, nodeManagersCache, zkClient);
    super.stop();
  }

  protected void handleEvent(ZKEvent event) {
    synchronized (lock) {
      if (event.getType() == ZKEventType.NODES_REGISTER) {
        final NodesRegisterEvent e = (NodesRegisterEvent) event;

        final JobId jobId = e.getJobId();
        final List<NodeId> nodeIdList = e.getNodeIdList();

        final String zkPath = ZKPaths.makePath(
          ZKPaths.makePath(this.zkRoot, jobId.toString()),
          "shuffle");

        try {
          for (NodeId nodeId : nodeIdList) {
            this.zkClient.create().creatingParentsIfNeeded().forPath(
              ZKPaths.makePath(zkPath, nodeId.toString()));
          }
        } catch (Exception ex) {
          LOG.error("nodes register failure ", ex);
        }

      } else if (event.getType() == ZKEventType.NODE_RENEW) {
        final NodeRenewEvent e = (NodeRenewEvent) event;

        final JobId jobId = e.getJobId();
        final NodeId nodeId = e.getNodeId();

        final String zkPath = ZKPaths.makePath(
          ZKPaths.makePath(
            ZKPaths.makePath(this.zkRoot, jobId.toString()),
            "shuffle"),
          nodeId.toString());

        try {
          this.zkClient.create().creatingParentsIfNeeded().forPath(zkPath);
        } catch (Exception ex) {
          LOG.error("node renew failure ", ex);
        }
      } else {
        throw new IllegalStateException(
          "unknown event type " + event.getType());
      }
    }
  }

  private class NMPathChildrenCacheListener
      implements PathChildrenCacheListener {
    @Override
    public void childEvent(
        CuratorFramework client,
        PathChildrenCacheEvent event) throws Exception {
      if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
        for (Job job : context.getAllJobs().values()) {
          final String shufflePath =
            ZKPaths.makePath(
              ZKPaths.makePath(
                ZKPaths.makePath(zkRoot, "shuffle"), job.getID().toString()),
              ZKPaths.getNodeFromPath(event.getData().getPath()));

          zkClient.delete().guaranteed().forPath(shufflePath);
        }
      }
    }
  }

}
