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
package org.apache.hadoop.realtime.child;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.protocol.DragonChildProtocol;
import org.apache.hadoop.realtime.protocol.records.FatalErrorRequest;
import org.apache.hadoop.realtime.protocol.records.FsErrorRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.PingRequest;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskInChild;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.realtime.security.TokenCache;
import org.apache.hadoop.realtime.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class ChildServiceDelegate {

  private static final Log LOG = LogFactory.getLog(ChildServiceDelegate.class);

  private DragonChildProtocol proxy = null;
  private final String jobId;
  private final Configuration conf;
  private final InetSocketAddress amAddress;
  private RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  YarnRPC rpc;

  public ChildServiceDelegate(Configuration conf, String jobId,
      InetSocketAddress amAddress) {
    this.conf = conf;
    this.jobId = jobId;
    this.amAddress = amAddress;
    UserGroupInformation.setConfiguration(this.conf);
    this.rpc = YarnRPC.create(conf);
  }

  public TaskInChild getTask(String containerIdString)
      throws YarnRemoteException {
    GetTaskRequest request =
        recordFactory.newRecordInstance(GetTaskRequest.class);
    request.setContainerId(containerIdString);
    TaskInChild task =
        ((GetTaskResponse) invoke("getTask", GetTaskRequest.class, request))
            .getTask();
    return task;
  }

  public boolean statusUpdate(TaskAttemptId jobId, TaskReport taskReport)
      throws YarnRemoteException {
    StatusUpdateRequest request =
        recordFactory.newRecordInstance(StatusUpdateRequest.class);
    request.setTaskAttemptId(jobId);
    request.setTaskStatus(taskReport);
    boolean result =
        ((StatusUpdateResponse) invoke("statusUpdate",
            StatusUpdateRequest.class, request)).getResult();
    return result;
  }

  public boolean ping(TaskAttemptId jobId) throws YarnRemoteException {
    PingRequest request = recordFactory.newRecordInstance(PingRequest.class);
    request.setTaskAttemptId(jobId);
    boolean result =
        ((PingResponse) invoke("ping", PingRequest.class, request)).getResult();
    return result;
  }

  public void fatalError(TaskAttemptId attemptId, String msg) throws YarnRemoteException {
    FatalErrorRequest request = recordFactory.newRecordInstance(FatalErrorRequest.class);
    request.setTaskAttemptId(attemptId);
    invoke("fatalError", FatalErrorRequest.class, request);
  }
  
  public void fsError(TaskAttemptId attemptId, String msg) throws YarnRemoteException{
    FsErrorRequest request = recordFactory.newRecordInstance(FsErrorRequest.class);
    request.setTaskAttemptId(attemptId);
    invoke("fatalError", FsErrorRequest.class, request);
  }

  private DragonChildProtocol getProxy() throws YarnException {
    if (proxy != null) {
      return proxy;
    }
    try {
      Token<JobTokenIdentifier> jt = loadCredentials(conf, amAddress);
      UserGroupInformation taskOwner =
          UserGroupInformation.createRemoteUser(jobId);
      taskOwner.addToken(jt);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to " + amAddress);
      }
      proxy =
          taskOwner.doAs(new PrivilegedExceptionAction<DragonChildProtocol>() {
            @Override
            public DragonChildProtocol run() throws IOException {
              return instantiateAMProxy(amAddress);
            }
          });
      return proxy;
    } catch (IOException e) {
      LOG.info("Could not connect to " + amAddress.toString(), e);
    } catch (InterruptedException e) {
      LOG.warn("getProxy() call interruped", e);
      throw new YarnException(e);
    }
    return null;
  }

  public void stopProxy() {
    if (proxy != null)
      this.rpc.stopProxy(proxy, conf);
  }

  DragonChildProtocol instantiateAMProxy(final InetSocketAddress amAddress)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to ApplicationMaster at: " + amAddress.toString());
    }
    DragonChildProtocol proxy =
        (DragonChildProtocol) rpc.getProxy(DragonChildProtocol.class,
            amAddress, conf);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connected to ApplicationMaster at: " + amAddress.toString());
    }
    return proxy;
  }

  private synchronized Object invoke(String method, Class<?> argClass,
      Object args) throws YarnRemoteException {
    Method methodOb = null;
    try {
      methodOb = DragonChildProtocol.class.getMethod(method, argClass);
    } catch (SecurityException e) {
      throw new YarnException(e);
    } catch (NoSuchMethodException e) {
      throw new YarnException("Method name mismatch", e);
    }
    while (true) {
      try {
        return methodOb.invoke(getProxy(), args);
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof YarnRemoteException) {
          LOG.warn("Error from remote end: "
              + e.getTargetException().getLocalizedMessage());
          if (LOG.isDebugEnabled())
            LOG.debug("Tracing remote error ", e.getTargetException());
          throw (YarnRemoteException) e.getTargetException();
        }
        if (LOG.isDebugEnabled())
          LOG.debug("Failed to contact AM for job " + jobId + " retrying..",
              e.getTargetException());
      } catch (Exception e) {
        if (LOG.isDebugEnabled())
          LOG.debug("Failed to contact AM for job " + jobId + "  Will retry..",
              e);
      }
    }
  }

  private static Token<JobTokenIdentifier> loadCredentials(Configuration conf,
      InetSocketAddress address) throws IOException {
    // load token cache storage
    String tokenFileLocation =
        System.getenv(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME);
    FileSystem fs = FileSystem.getLocal(conf);
    String jobTokenFile =
        new Path(tokenFileLocation)
            .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toUri()
            .getPath();
    Credentials credentials = TokenCache.loadTokens(jobTokenFile, conf);
    LOG.debug("loading token. # keys =" + credentials.numberOfSecretKeys()
        + "; from file=" + jobTokenFile);
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    jt.setService(new Text(address.getAddress().getHostAddress() + ":"
        + address.getPort()));
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);
    for (Token<? extends TokenIdentifier> tok : credentials.getAllTokens()) {
      current.addToken(tok);
    }
    return jt;
  }

}
