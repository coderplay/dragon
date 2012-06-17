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

package org.apache.hadoop.realtime.server;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressRequest;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.PingRequest;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.security.authorize.DragonAMPolicyProvider;
import org.apache.hadoop.realtime.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;

/**
 * This module is responsible for talking to the jobclient (user facing).
 * 
 */
public class DragonChildService extends CompositeService implements
    TaskAttemptListener {

  static final Log LOG = LogFactory.getLog(DragonChildService.class);

  private DragonChildProtocol protocolHandler;
  protected TaskHeartbeatHandler taskHeartbeatHandler;
  private Server server;
  private InetSocketAddress bindAddress;
  
  private ConcurrentMap<String, Task> containerIDToActiveAttemptMap =
      new ConcurrentHashMap<String, Task>();
  private Set<String> launchedJVMs = Collections
      .newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  
  private AppContext context;

  private JobTokenSecretManager jobTokenSecretManager = null;

  public DragonChildService(AppContext appContext,
      JobTokenSecretManager jobTokenSecretManager) {
    super("DragonChildService");
    this.context = appContext;
    this.jobTokenSecretManager = jobTokenSecretManager;
    this.protocolHandler = new DragonChildProtocolHandler();
  }

  @Override
  public void init(Configuration conf) {
    registerHeartbeatHandler(conf);
    super.init(conf);
  }
  
  protected void registerHeartbeatHandler(Configuration conf) {
    taskHeartbeatHandler =
        new TaskHeartbeatHandler(context.getEventHandler(), context.getClock(),
            conf.getInt(DragonJobConfig.DRAGON_AM_TASK_LISTENER_THREAD_COUNT,
                DragonJobConfig.DEFAULT_DRAGON_AM_TASK_LISTENER_THREAD_COUNT));
    addService(taskHeartbeatHandler);
  }
  
  public void start() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    InetSocketAddress address = new InetSocketAddress(0);

    server =
        rpc.getServer(DragonChildProtocol.class, protocolHandler, address,
            conf, jobTokenSecretManager, conf.getInt(
                DragonJobConfig.DRAGON_AM_TASK_LISTENER_THREAD_COUNT,
                DragonJobConfig.DEFAULT_DRAGON_AM_TASK_LISTENER_THREAD_COUNT));

    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
      refreshServiceAcls(conf, new DragonAMPolicyProvider());
    }

    server.start();
    this.bindAddress = NetUtils.getConnectAddress(server);
    LOG.info("Instantiated DRAGONClientService at " + this.bindAddress);
    super.start();
  }

  void refreshServiceAcls(Configuration configuration,
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  public void stop() {
    server.stop();

    super.stop();
  }

  class DragonChildProtocolHandler implements DragonChildProtocol {

    private RecordFactory recordFactory = RecordFactoryProvider
        .getRecordFactory(null);

    @Override
    public GetTaskResponse getTask(GetTaskRequest request)
        throws YarnRemoteException {
      String containerId = request.getContainerId();
      LOG.info("Container with ID : " + containerId + " asked for a task");
      
      if (!containerIDToActiveAttemptMap.containsKey(containerId)) {
        LOG.info("Container with ID: " + containerId + " is invalid and will be killed.");
        //jvmTask = TASK_FOR_INVALID_JVM;
      } else {
        if (!launchedJVMs.contains(containerId)) {
          LOG.info("Container with ID: " + containerId
              + " asking for task before AM launch registered. Given null task");
        } else {

          Task task =containerIDToActiveAttemptMap.remove(containerId);
          launchedJVMs.remove(containerId);
          LOG.info("Container with ID: " + containerId + " given task: " + task.getID());
        }
      }
      // TODO: return the real word child need to run.
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StatusUpdateResponse statusUpdate(StatusUpdateRequest request)
        throws YarnRemoteException {
      TaskAttemptId attemptId = request.getTaskAttemptId();
      TaskReport report = request.getTaskReport();
      LOG.info("Status update from " + attemptId.toString());
      taskHeartbeatHandler.receivedPing(attemptId);
      TaskAttemptStatus taskAttemptStatus =
          new TaskAttemptStatus();
      taskAttemptStatus.id = attemptId;

      taskAttemptStatus.progress = report.getProgress();
      LOG.info("Progress of TaskAttempt " + attemptId + " is : "
          + report.getProgress());

      taskAttemptStatus.stateString = report.getTaskState().toString();

      taskAttemptStatus.counters = report.getCounters();

      context.getEventHandler().handle(
          new TaskAttemptStatusUpdateEvent(taskAttemptStatus.id,
              taskAttemptStatus));
      StatusUpdateResponse response =
          recordFactory.newRecordInstance(StatusUpdateResponse.class);
      response.setResult(true);
      return response;
    }

    @Override
    public PingResponse ping(PingRequest request) throws YarnRemoteException {
      TaskAttemptId attemptId = request.getTaskAttemptId();
      LOG.info("Ping from " + attemptId.toString());
      taskHeartbeatHandler.receivedPing(attemptId);
      PingResponse response = recordFactory.newRecordInstance(PingResponse.class);
      response.setResult(true);
      return response;
    }

    @Override
    public GetShuffleAddressResponse getShuffleAddress(
        GetShuffleAddressRequest request) throws YarnRemoteException {
      // TODO: return the shuffle Address.
      return null;
    }

  }

  @Override
  public InetSocketAddress getAddress() {
    return bindAddress;
  }

  @Override
  public void registerPendingTask(Task task, String containerId) {
    containerIDToActiveAttemptMap.put(containerId, task);

  }

  @Override
  public void registerLaunchedTask(TaskAttemptId attemptID, String containerId) {
    launchedJVMs.add(containerId);
    taskHeartbeatHandler.register(attemptID);

  }

  @Override
  public void unregister(TaskAttemptId attemptID, String containerId) {
    launchedJVMs.remove(containerId);
    containerIDToActiveAttemptMap.remove(containerId);

    // unregister this attempt
    taskHeartbeatHandler.unregister(attemptID);
  }
}
