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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.app.rm.ContainerAllocator;
import org.apache.hadoop.realtime.app.rm.ContainerAllocatorEvent;
import org.apache.hadoop.realtime.app.rm.RMContainerAllocator;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.job.app.event.ChildExecutionEvent;
import org.apache.hadoop.realtime.job.app.event.ChildExecutionEventType;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptEventType;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.FatalErrorRequest;
import org.apache.hadoop.realtime.protocol.records.FatalErrorResponse;
import org.apache.hadoop.realtime.protocol.records.FsErrorRequest;
import org.apache.hadoop.realtime.protocol.records.FsErrorResponse;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressRequest;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.PingRequest;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.realtime.records.ChildExecutionContext;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.security.authorize.DragonAMPolicyProvider;
import org.apache.hadoop.realtime.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;

/**
 * This module is responsible for talking to the remote child process.
 * 
 */
public class DragonChildService extends CompositeService implements
    ChildService {

  static final Log LOG = LogFactory.getLog(DragonChildService.class);

  private DragonChildProtocol protocolHandler;
  protected TaskHeartbeatHandler taskHeartbeatHandler;
  private Server server;
  private InetSocketAddress bindAddress;

  private ConcurrentMap<ContainerId, ChildExecutionContext> containerIDToActiveAttemptMap =
      new ConcurrentHashMap<ContainerId, ChildExecutionContext>();
  private Set<ContainerId> launchedJVMs = Collections
      .newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  private AppContext context;

  private JobTokenSecretManager jobTokenSecretManager = null;

  BlockingQueue<ChildExecutionEvent> eventQueue =
      new LinkedBlockingQueue<ChildExecutionEvent>();
  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

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
    InetSocketAddress address = NetUtils.createSocketAddr("0.0.0.0:0");
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }

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
    this.bindAddress =
        NetUtils.createSocketAddr(hostNameResolved.getHostAddress() + ":"
            + server.getPort());
    LOG.info("Instantiated DRAGONClientService at " + this.bindAddress);

    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        ChildExecutionEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = DragonChildService.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }
          handleEvent(event);
        }
      }
    };
    this.eventHandlingThread.start();
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

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ChildExecutionEvent event) {
    if (event.getType() == ChildExecutionEventType.CHILDTASK_ASSIGNED) {
      containerIDToActiveAttemptMap.put(event.getContainerId(), event.getChildTask());
    } else if (event.getType() == ChildExecutionEventType.CHILDTASK_LAUNCHED) {
      launchedJVMs.add(event.getContainerId());
      taskHeartbeatHandler.register(event.getTaskAttemptId());
    } else {
      launchedJVMs.remove(event.getTaskAttemptId());
      containerIDToActiveAttemptMap.remove(event.getContainerId());
      // unregister this attempt
      taskHeartbeatHandler.unregister(event.getTaskAttemptId());
    }
  }
  
  @Override
  public void handle(ChildExecutionEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "of RMContainerAllocator: " + remCapacity);
    }
    try {
      eventQueue.put(event);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
  }

  class DragonChildProtocolHandler implements DragonChildProtocol {

    private RecordFactory recordFactory = RecordFactoryProvider
        .getRecordFactory(null);

    @Override
    public GetTaskResponse getTask(GetTaskRequest request)
        throws YarnRemoteException {
      ContainerId containerId = request.getContainerId();
      LOG.info("Container with ID : " + containerId + " asked for a task");
      ChildExecutionContext task = null;
      if (!containerIDToActiveAttemptMap.containsKey(containerId)) {
        LOG.info("Container with ID: " + containerId
            + " is invalid and will be killed.");
        // jvmTask = TASK_FOR_INVALID_JVM;
      } else {
        if (!launchedJVMs.contains(containerId)) {
          LOG.info("Container with ID: " + containerId
              + " asking for task before AM launch registered. Given null task");
        } else {

          task =
              (ChildExecutionContext) containerIDToActiveAttemptMap
                  .remove(containerId);
          launchedJVMs.remove(containerId);
          LOG.info("Container with ID: " + containerId + " given task attempt: "
              + task.getTaskAttemptId());
        }
      }
      GetTaskResponse response =
          recordFactory.newRecordInstance(GetTaskResponse.class);
      response.setTask(task);
      return response;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StatusUpdateResponse statusUpdate(StatusUpdateRequest request)
        throws YarnRemoteException {
      TaskAttemptId attemptId = request.getTaskAttemptId();
      TaskReport report = request.getTaskReport();
      LOG.info("Status update from " + attemptId.toString());
      taskHeartbeatHandler.receivedPing(attemptId);
      TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatus();
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
      PingResponse response =
          recordFactory.newRecordInstance(PingResponse.class);
      response.setResult(true);
      return response;
    }

    @Override
    public GetShuffleAddressResponse getShuffleAddress(
        GetShuffleAddressRequest request) throws YarnRemoteException {
      return null;
    }

    @Override
    public FsErrorResponse fsError(FsErrorRequest request)
        throws YarnRemoteException {
      TaskAttemptId attemptId = request.getTaskAttemptId();
      String errorMessage = request.getMessage();
      LOG.fatal("Task: " + attemptId + " - failed due to FSError: "
          + errorMessage);
      reportDiagnosticInfo(attemptId, "FSError: " + errorMessage);

      context.getEventHandler().handle(
          new TaskAttemptEvent(attemptId, TaskAttemptEventType.TA_FAILMSG));
      FsErrorResponse response =
          recordFactory.newRecordInstance(FsErrorResponse.class);
      return response;
    }

    @Override
    public FatalErrorResponse fatalError(FatalErrorRequest request)
        throws YarnRemoteException {
      TaskAttemptId attemptId = request.getTaskAttemptId();
      String errorMessage = request.getMessage();
      LOG.fatal("Task: " + attemptId + " - failed due to FSError: "
          + errorMessage);
      reportDiagnosticInfo(attemptId, "Error: " + errorMessage);

      context.getEventHandler().handle(
          new TaskAttemptEvent(attemptId, TaskAttemptEventType.TA_FAILMSG));
      FatalErrorResponse response =
          recordFactory.newRecordInstance(FatalErrorResponse.class);
      return response;
    }

  }

  @Override
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  
  public void reportDiagnosticInfo(TaskAttemptId attemptId,
      String diagnosticInfo){
    LOG.info("Diagnostics report from " + attemptId.toString() + ": "
        + diagnosticInfo);

    context.getEventHandler().handle(
        new TaskAttemptDiagnosticsUpdateEvent(attemptId, diagnosticInfo));
  }
}
