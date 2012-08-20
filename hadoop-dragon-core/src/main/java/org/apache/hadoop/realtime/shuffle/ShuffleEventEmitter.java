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
package org.apache.hadoop.realtime.shuffle;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.event.EventEmitter;
import org.apache.hadoop.realtime.mr.Partitioner;
import org.apache.hadoop.realtime.util.NamedThreadFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 */
public class ShuffleEventEmitter<KEY, VALUE> implements
    EventEmitter<KEY, VALUE> {
  private static final Log LOG = LogFactory.getLog(ShuffleEventEmitter.class);
  
  private final ClientBootstrap bootstrap;
  private final Partitioner<KEY, VALUE> partitioner;
  private final int partitions;

  private List<ChannelFuture> futures;

  private int[] failures;

  @SuppressWarnings("unchecked")
  ShuffleEventEmitter(Configuration conf, List<InetSocketAddress> clients)
      throws IOException, ClassNotFoundException {
    partitions = clients.size();
    if (partitions > 1) {
      partitioner =
          (Partitioner<KEY, VALUE>) ReflectionUtils.newInstance(
              conf.getClassByName(DragonJobConfig.JOB_EVENT_EMITTER_CLASS),
              conf);
    } else {
      partitioner = new Partitioner<KEY, VALUE>() {
        @Override
        public int getPartition(Event<KEY, VALUE> event, int numPartitions) {
          return partitions - 1;
        }
      };
    }
    
    failures = new int[partitions];
    for(int i = 0; i < partitions; i++) {
      failures[i] = 0;
    }

    ThreadFactory bossThreadFactory = new NamedThreadFactory("shuffle-boss-");
    ThreadFactory workerThreadFactory =
        new NamedThreadFactory("shuffle-worker-");
    bootstrap =
        new ClientBootstrap(new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(bossThreadFactory),
            Executors.newCachedThreadPool(workerThreadFactory)));
    bootstrap.setOption("tcpNoDelay",
        conf.getBoolean("dragon.shuffle.tcp.nodelay", true));
    bootstrap.setOption("reuseAddress",
        conf.getBoolean("dragon.shuffle.tcp.reuseaddress", true));

    ShuffleClientHandler handler = new ShuffleClientHandler();
    bootstrap.setPipelineFactory(
        new ShuffleClientPipelineFactory(conf, handler));
    
    int timeout = conf.getInt("dragon.shuffle.timeout", 1000);
    for (InetSocketAddress client : clients) {
      ChannelFuture future = bootstrap.connect(client);
      future.awaitUninterruptibly(timeout);
      futures.add(future);
    }

  }

  @Override
  public boolean emitEvent(final Event<KEY, VALUE> event) throws IOException,
      InterruptedException { 
    int partition = partitioner.getPartition(event, partitions);    
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + event + " (" +
          partition + ")");
    }
    
    // check the corresponding nodemanager is healthy
    boolean isRemoteNNHealthy = true;
    // if remote nodemanager is not healthy or there are previous events 
    // failed in sending, then save the event to hdfs files.
    if(!isRemoteNNHealthy || failures[partition] > 0) {
      saveEventToLocal(event);
    } else {
      final long beginTime = System.currentTimeMillis();
      final Channel channel = futures.get(partition).getChannel();
      ChannelFuture writeFuture = futures.get(partition).getChannel().write(event);
      writeFuture.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            return;
          }
          String errorMsg = "";
          // write timeout
          if (System.currentTimeMillis() - beginTime >= 100) {
            errorMsg = "write to send buffer consume too long time("
                    + (System.currentTimeMillis() - beginTime)
                    + "),request id is:" + event.offset();
          }
          if (future.isCancelled()) {
            errorMsg = "Send request to " + channel.toString()
                    + " cancelled by user,request id is:" + event.offset();
          }
          if (!future.isSuccess()) {
            if (channel.isConnected()) {
              // maybe some exception,so close the channel
              channel.close();
            } else {
              // remove client?
            }
            errorMsg = "Send request to " + channel.toString() + " error"
                    + future.getCause();
          }
          LOG.error(errorMsg);
          // response??
        }
        
      });
      
    }
    return false;
  }

  
  private void saveEventToLocal(Event<KEY, VALUE> event) throws IOException {
    
  }

  @Override
  public boolean emitEvent(Event<KEY, VALUE> event, 
                           long timeout,
                           TimeUnit unit)
          throws IOException, InterruptedException {

    return false;
  }

  @Override
  public void close() throws IOException {
    
  }

}
