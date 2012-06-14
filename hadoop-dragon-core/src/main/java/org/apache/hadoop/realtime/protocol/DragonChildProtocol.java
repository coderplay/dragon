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

package org.apache.hadoop.realtime.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressRequest;
import org.apache.hadoop.realtime.protocol.records.GetShuffleAddressResponse;
import org.apache.hadoop.realtime.protocol.records.GetTaskRequest;
import org.apache.hadoop.realtime.protocol.records.GetTaskResponse;
import org.apache.hadoop.realtime.protocol.records.PingRequest;
import org.apache.hadoop.realtime.protocol.records.PingResponse;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateRequest;
import org.apache.hadoop.realtime.protocol.records.StatusUpdateResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

/**
 * Protocol that task child process uses to contact its parent process. The
 * parent is a daemon which which polls the central master for a new map or
 * reduce task and runs it as a child process. All communication between child
 * and parent is via this protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface DragonChildProtocol {

  public static final long versionID = 19L;

  /**
   * Called when a child task process starts, to get its task.
   * 
   * @param context the JvmContext of the JVM w.r.t the TaskTracker that
   *          launched it
   * @return Task object
   * @throws IOException
   */
  GetTaskResponse getTask(GetTaskRequest request) throws YarnRemoteException;

  /**
   * Report child's progress to parent.
   * 
   * @param taskId task-id of the child
   * @param taskStatus status of the child
   * @throws IOException
   * @throws InterruptedException
   * @return True if the task is known
   */
  StatusUpdateResponse statusUpdate(StatusUpdateRequest request)
      throws YarnRemoteException;

  /**
   * Periodically called by child to check if parent is still alive.
   * 
   * @return True if the task is known
   */
  PingResponse ping(PingRequest request) throws YarnRemoteException;

  /**
   * Called by a reduce task to get the map output locations for finished maps.
   * Returns an update centered around the map-task-completion-events. The
   * update also piggybacks the information whether the events copy at the
   * task-tracker has changed or not. This will trigger some action at the
   * child-process.
   * 
   * @param fromIndex the index starting from which the locations should be
   *          fetched
   * @param maxLocs the max number of locations to fetch
   * @param id The attempt id of the task that is trying to communicate
   * @return A {@link MapTaskCompletionEventsUpdate}
   */
  GetShuffleAddressResponse getShuffleAddress(GetShuffleAddressRequest request)
      throws YarnRemoteException;

}
