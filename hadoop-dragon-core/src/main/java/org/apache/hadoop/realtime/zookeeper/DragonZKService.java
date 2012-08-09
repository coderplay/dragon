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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * class description goes here.
 */
public class DragonZKService extends AbstractService
    implements EventHandler<ZKEvent> {

  protected final BlockingQueue<ZKEvent> eventQueue =
      new LinkedBlockingQueue<ZKEvent>();

  private static final Log LOG = LogFactory.getLog(DragonZKService.class);
  private Thread eventHandlingThread;
  private volatile boolean stopped;

  private final Object lock = new Object();

  private final AppContext context;

  private DragonZooKeeper dragonZK;

  public DragonZKService(AppContext context) {
    super("DragonZKService");

    this.context = context;
  }

  @Override
  public synchronized void init(Configuration config) {
    final String serverList = config.get(DragonConfiguration.ZK_SERVER_LIST);
    final String zkRoot = config.get(DragonConfiguration.ZK_ROOT);

    try {
      CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
        serverList,
        new ExponentialBackoffRetry(300, 5));

      this.dragonZK = new DragonZooKeeper(zkClient, zkRoot);

      PathChildrenCache nodeManagersCache = new PathChildrenCache(
        dragonZK.getZkClient(),
        dragonZK.getNodeManagersPath(),
        false);

      nodeManagersCache.getListenable().addListener(
        dragonZK.new NMPathChildrenCacheListener(context));

      dragonZK.setNodeManagersCache(nodeManagersCache);

      for (Job job : context.getAllJobs().values()) {
        final JobId jobId = job.getID();
        final PathChildrenCache shuffleNodeCache = new PathChildrenCache(
          zkClient,
          dragonZK.getShufflePath(jobId),
          true);
        dragonZK.addShuffleNodeCache(jobId, shuffleNodeCache);
      }

    } catch (Exception e) {
      throw new IllegalStateException("init app master zookeeper failure", e);
    }

    super.init(config);
  }

  @Override
  public synchronized void start() {

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
    LOG.info("Stopping DragonZKService. "
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

    IOUtils.cleanup(LOG, dragonZK);

    super.stop();
  }

  protected void handleEvent(ZKEvent event) {
    synchronized (lock) {
      ZKEventType type = event.getType();
      switch (type) {
        case NODES_REGISTER:
          final NodesRegisterEvent registerEvent = (NodesRegisterEvent) event;
          dragonZK.registerTasks(
            registerEvent.getJobId(),
            registerEvent.getNodeList());
          break;
        case NODE_RENEW:
          final NodeRenewEvent renewEvent = (NodeRenewEvent) event;
          dragonZK.renewNode(renewEvent.getJobId(),
            renewEvent.getNodeId(), renewEvent.getTaskId());
          break;
        default:
          throw new IllegalStateException(
            "unknown event type " + event.getType());
      }

    }
  }


}
