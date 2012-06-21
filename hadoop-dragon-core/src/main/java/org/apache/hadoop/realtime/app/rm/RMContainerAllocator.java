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
package org.apache.hadoop.realtime.app.rm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.app.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.realtime.job.app.event.JobEvent;
import org.apache.hadoop.realtime.job.app.event.JobEventType;
import org.apache.hadoop.realtime.job.app.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.server.ClientService;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;

public class RMContainerAllocator extends RMContainerRequestor implements
    ContainerAllocator {

  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.class);
  private long retryInterval;
  private long retrystartTime;

  private int containersAllocated = 0;
  private int containersReleased = 0;

  private int taskResourceReqt = 0;

  private Thread eventHandlingThread;
  private volatile boolean stopEventHandling;

  private static final Priority PRIORITY_TASK;

  BlockingQueue<ContainerAllocatorEvent> eventQueue =
      new LinkedBlockingQueue<ContainerAllocatorEvent>();

  // holds information about the assigned containers to task attempts
  private final AssignedRequests assignedRequests = new AssignedRequests();

  // holds scheduled requests to be fulfilled by RM
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();

  static {
    PRIORITY_TASK =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
            Priority.class);
    PRIORITY_TASK.setPriority(20);
  }

  public RMContainerAllocator(AppContext context) {
    super(context);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    RackResolver.init(conf);
    retryInterval =
        getConfig().getLong(DragonJobConfig.DRAGON_AM_TO_RM_WAIT_INTERVAL_MS,
            DragonJobConfig.DEFAULT_DRAGON_AM_TO_RM_WAIT_INTERVAL_MS);
    // Init startTime to current time. If all goes well, it will be reset after
    // first attempt to contact RM.
    retrystartTime = System.currentTimeMillis();
  }

  @Override
  public void start() {
    this.eventHandlingThread = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {

        ContainerAllocatorEvent event;

        while (!stopEventHandling && !Thread.currentThread().isInterrupted()) {
          try {
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return;
          }

          try {
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // Kill the AM
            eventHandler.handle(new JobEvent(getJob().getID(),
                JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.start();
  }

  @Override
  protected synchronized void heartbeat() throws Exception {
    // LOG.info("Before Scheduling: " + getStat());
    List<Container> allocatedContainers = getResources();
    // LOG.info("After Scheduling: " + getStat());
    if (allocatedContainers.size() > 0) {
      LOG.info("Before Assign: " + getStat());
      scheduledRequests.assign(allocatedContainers);
      LOG.info("After Assign: " + getStat());
    }
  }

  /**
   * Synchronized to avoid findbugs warnings
   */
  private synchronized String getStat() {
    return " ScheduledTasks:" + scheduledRequests.tasks.size()
        + " AssignedTasks:" + assignedRequests.tasks.size()
        + " containersAllocated:" + containersAllocated
        + " containersReleased:" + containersReleased
        + " availableResources(headroom):" + getAvailableResources();
  }

  @SuppressWarnings("unchecked")
  private List<Container> getResources() throws Exception {
    int headRoom =
        getAvailableResources() != null ? getAvailableResources().getMemory()
            : 0;// first time it would be null
    AMResponse response;
    /*
     * If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
     * milliseconds before aborting. During this interval, AM will still try to
     * contact the RM.
     */
    try {
      response = makeRemoteRequest();
      // Reset retry count if no exception occurred.
      retrystartTime = System.currentTimeMillis();
    } catch (Exception e) {
      // This can happen when the connection to the RM has gone down. Keep
      // re-trying until the retryInterval has expired.
      if (System.currentTimeMillis() - retrystartTime >= retryInterval) {
        LOG.error("Could not contact RM after " + retryInterval
            + " milliseconds.");
        eventHandler.handle(new JobEvent(this.getJob().getID(),
            JobEventType.INTERNAL_ERROR));
        throw new YarnException("Could not contact RM after " + retryInterval
            + " milliseconds.");
      }
      // Throw this up to the caller, which may decide to ignore it and
      // continue to attempt to contact the RM.
      throw e;
    }
    if (response.getReboot()) {
      // This can happen if the RM has been restarted. If it is in that state,
      // this application must clean itself up.
      eventHandler.handle(new JobEvent(this.getJob().getID(),
          JobEventType.INTERNAL_ERROR));
      throw new YarnException("Resource Manager doesn't recognize AttemptId: "
          + this.getContext().getApplicationID());
    }
    int newHeadRoom =
        getAvailableResources() != null ? getAvailableResources().getMemory()
            : 0;
    List<Container> newContainers = response.getAllocatedContainers();
    List<ContainerStatus> finishedContainers =
        response.getCompletedContainersStatuses();
    if (newContainers.size() + finishedContainers.size() > 0
        || headRoom != newHeadRoom) {
      // something changed
    }

    if (LOG.isDebugEnabled()) {
      for (Container cont : newContainers) {
        LOG.debug("Received new Container :" + cont);
      }
    }

    // Called on each allocation. Will know about newly blacklisted/added hosts.
    computeIgnoreBlacklisting();

    return newContainers;
  }

  @SuppressWarnings({ "unchecked" })
  protected synchronized void handleEvent(ContainerAllocatorEvent event) {
    if (event.getType() == ContainerAllocator.EventType.CONTAINER_REQ) {
      ContainerRequestEvent reqEvent = (ContainerRequestEvent) event;
      JobId jobId = getJob().getID();
      int supportedMaxContainerCapability =
          getMaxContainerCapability().getMemory();
      taskResourceReqt = reqEvent.getCapability().getMemory();
      int minSlotMemSize = getMinContainerCapability().getMemory();
      taskResourceReqt =
          (int) Math.ceil((float) taskResourceReqt / minSlotMemSize)
              * minSlotMemSize;
      LOG.info("mapResourceReqt:" + taskResourceReqt);
      if (taskResourceReqt > supportedMaxContainerCapability) {
        String diagMsg =
            "MAP capability required is more than the supported "
                + "max container capability in the cluster. Killing the Job. mapResourceReqt: "
                + taskResourceReqt + " maxContainerCapability:"
                + supportedMaxContainerCapability;
        LOG.info(diagMsg);
        eventHandler.handle(new JobDiagnosticsUpdateEvent(jobId, diagMsg));
        eventHandler.handle(new JobEvent(jobId, JobEventType.JOB_KILL));
      }
      // set the rounded off memory
      reqEvent.getCapability().setMemory(taskResourceReqt);
      scheduledRequests.addTask(reqEvent);// maps are immediately scheduled
    } else if (event.getType() == ContainerAllocator.EventType.CONTAINER_DEALLOCATE) {

      LOG.info("Processing the event " + event.toString());

      TaskAttemptId aId = event.getAttemptID();

      boolean removed = scheduledRequests.remove(aId);
      if (!removed) {
        ContainerId containerId = assignedRequests.get(aId);
        if (containerId != null) {
          removed = true;
          assignedRequests.remove(aId);
          containersReleased++;
          release(containerId);
        }
      }
      if (!removed) {
        LOG.error("Could not deallocate container for task attemptId " + aId);
      }
    } else if (event.getType() == ContainerAllocator.EventType.CONTAINER_FAILED) {
      ContainerFailedEvent fEv = (ContainerFailedEvent) event;
      String host = getHost(fEv.getContMgrAddress());
      containerFailedOnHost(host);
    }
  }

  private static String getHost(String contMgrAddress) {
    String host = contMgrAddress;
    String[] hostport = host.split(":");
    if (hostport.length == 2) {
      host = hostport[0];
    }
    return host;
  }

  private class ScheduledRequests {

    private final Map<TaskAttemptId, ContainerRequest> tasks =
        new LinkedHashMap<TaskAttemptId, ContainerRequest>();

    boolean remove(TaskAttemptId tId) {
      ContainerRequest req = null;
      req = tasks.remove(tId);

      if (req == null) {
        return false;
      } else {
        decContainerReq(req);
        return true;
      }
    }

    void addTask(ContainerRequestEvent event) {
      ContainerRequest request = null;
      request = new ContainerRequest(event, PRIORITY_TASK);
      tasks.put(event.getAttemptID(), request);
      addContainerReq(request);
    }

    @SuppressWarnings("unchecked")
    private void assign(List<Container> allocatedContainers) {
      Iterator<Container> it = allocatedContainers.iterator();
      LOG.info("Got allocated containers " + allocatedContainers.size());
      containersAllocated += allocatedContainers.size();
      while (it.hasNext()) {
        Container allocated = it.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated.getId()
              + " with priority " + allocated.getPriority() + " to NM "
              + allocated.getNodeId());
        }

        // check if allocated container meets memory requirements
        // and whether we have any scheduled tasks that need
        // a container to be assigned
        boolean isAssignable = true;
        Priority priority = allocated.getPriority();
        int allocatedMemory = allocated.getResource().getMemory();
        if (PRIORITY_TASK.equals(priority)) {
          if (allocatedMemory < taskResourceReqt || tasks.isEmpty()) {
            LOG.info("Cannot assign container " + allocated
                + " for a map as either "
                + " container memory less than required " + taskResourceReqt
                + " or no pending map tasks - maps.isEmpty=" + tasks.isEmpty());
            isAssignable = false;
          }
        }
        boolean blackListed = false;
        ContainerRequest assigned = null;

        ContainerId allocatedContainerId = allocated.getId();
        if (isAssignable) {
          // do not assign if allocated container is on a
          // blacklisted host
          String allocatedHost = allocated.getNodeId().getHost();
          blackListed = isNodeBlacklisted(allocatedHost);
          if (blackListed) {
            // we need to request for a new container
            // and release the current one
            LOG.info("Got allocated container on a blacklisted " + " host "
                + allocatedHost + ". Releasing container " + allocated);

            // find the request matching this allocated container
            // and replace it with a new one
            ContainerRequest toBeReplacedReq =
                getContainerReqToReplace(allocated);
            if (toBeReplacedReq != null) {
              LOG.info("Placing a new container request for task attempt "
                  + toBeReplacedReq.attemptID);
              ContainerRequest newReq =
                  getFilteredContainerRequest(toBeReplacedReq);
              decContainerReq(toBeReplacedReq);
              tasks.put(newReq.attemptID, newReq);
              addContainerReq(newReq);
            } else {
              LOG.info("Could not map allocated container to a valid request."
                  + " Releasing allocated container " + allocated);
            }
          } else {
            assigned = assign(allocated);
            if (assigned != null) {
              // Update resource requests
              decContainerReq(assigned);

              // send the container-assigned event to task attempt
              eventHandler.handle(new TaskAttemptContainerAssignedEvent(
                  assigned.attemptID, allocated, applicationACLs));
              assignedRequests.add(allocatedContainerId, assigned.attemptID);

              if (LOG.isDebugEnabled()) {
                LOG.info("Assigned container (" + allocated + ") "
                    + " to task " + assigned.attemptID + " on node "
                    + allocated.getNodeId().toString());
              }
            } else {
              // not assigned to any request, release the container
              LOG.info("Releasing unassigned and invalid container "
                  + allocated + ". RM has gone crazy, someone go look!"
                  + " Hey RM, if you are so rich, go donate to non-profits!");
            }
          }
        }

        // release container if it was blacklisted
        // or if we could not assign it
        if (blackListed || assigned == null) {
          containersReleased++;
          release(allocatedContainerId);
        }
      }
    }

    private ContainerRequest assign(Container allocated) {
      ContainerRequest assigned = null;

      Priority priority = allocated.getPriority();
      if (PRIORITY_TASK.equals(priority)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Assigning container " + allocated + " to map");
        }
        assigned = assignToTask(allocated);
      } else {
        LOG.warn("Container allocated at unwanted priority: " + priority
            + ". Returning to RM...");
      }
      return assigned;
    }

    private ContainerRequest getContainerReqToReplace(Container allocated) {
      LOG.info("Finding containerReq for allocated container: " + allocated);
      Priority priority = allocated.getPriority();
      ContainerRequest toBeReplaced = null;
      if (PRIORITY_TASK.equals(priority)) {
        LOG.info("Replacing TASK container " + allocated.getId());
        TaskAttemptId tId = tasks.keySet().iterator().next();
        toBeReplaced = tasks.remove(tId);
      }
      LOG.info("Found replacement: " + toBeReplaced);
      return toBeReplaced;
    }

    private ContainerRequest assignToTask(Container allocated) {
      ContainerRequest assigned = null;
      // try to assign to reduces if present
      if (assigned == null && tasks.size() > 0) {
        TaskAttemptId tId = tasks.keySet().iterator().next();
        assigned = tasks.remove(tId);
        LOG.info("Assigned to reduce");
      }
      return assigned;
    }
  }

  private class AssignedRequests {
    private final Map<ContainerId, TaskAttemptId> containerToAttemptTask =
        new HashMap<ContainerId, TaskAttemptId>();
    private final LinkedHashMap<TaskAttemptId, ContainerId> tasks =
        new LinkedHashMap<TaskAttemptId, ContainerId>();

    void add(ContainerId containerId, TaskAttemptId tId) {
      LOG.info("Assigned container " + containerId.toString() + " to " + tId);
      containerToAttemptTask.put(containerId, tId);
      tasks.put(tId, containerId);
    }

    boolean remove(TaskAttemptId tId) {
      ContainerId containerId = null;
      containerId = tasks.remove(tId);
      if (containerId != null) {
        containerToAttemptTask.remove(containerId);
        return true;
      }
      return false;
    }

    ContainerId get(TaskAttemptId tId) {
      return tasks.get(tId);
    }
  }

  @Override
  public void handle(ContainerAllocatorEvent event) {
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
}
