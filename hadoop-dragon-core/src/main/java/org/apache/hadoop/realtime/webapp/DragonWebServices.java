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

package org.apache.hadoop.realtime.webapp;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.client.app.AppContext;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.job.TaskAttempt;
import org.apache.hadoop.realtime.records.*;
import org.apache.hadoop.realtime.webapp.dao.*;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;

@Path("/ws/v1/mapreduce")
public class DragonWebServices {
  private final AppContext appCtx;
  private final App app;
  private final Configuration conf;

  @Inject
  public DragonWebServices(final App app, final AppContext context,
                           final Configuration conf) {
    this.appCtx = context;
    this.app = app;
    this.conf = conf;
  }

  Boolean hasAccess(Job job, HttpServletRequest request) {
    /*String remoteUser = request.getRemoteUser();
    UserGroupInformation callerUGI = null;
    if (remoteUser != null) {
      callerUGI = UserGroupInformation.createRemoteUser(remoteUser);
    }
    if (callerUGI != null && !job.checkAccess(callerUGI, JobACL.VIEW_JOB)) {
      return false;
    }  */
    return true;
  }

  /**
   * convert a job id string to an actual job and handle all the error checking.
   */
 public static Job getJobFromJobIdString(String jid, AppContext appCtx) throws NotFoundException {
    JobId jobId;
    Job job;
    try {
      jobId = JobId.parseJobId(jid);
    } catch (YarnException e) {
      throw new NotFoundException(e.getMessage());
    }
    if (jobId == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    job = appCtx.getJob(jobId);
    if (job == null) {
      throw new NotFoundException("job, " + jid + ", is not found");
    }
    return job;
  }

  /**
   * convert a task id string to an actual task and handle all the error
   * checking.
   */
  public static Task getTaskFromTaskIdString(String tid, Job job) throws NotFoundException {
    TaskId taskID;
    Task task;
    try {
      taskID = TaskId.parseTaskId(tid);
    } catch (YarnException e) {
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    }
    if (taskID == null) {
      throw new NotFoundException("taskid " + tid + " not found or invalid");
    }
    task = job.getTask(taskID);
    if (task == null) {
      throw new NotFoundException("task not found with id " + tid);
    }
    return task;
  }

  /**
   * convert a task attempt id string to an actual task attempt and handle all
   * the error checking.
   */
  public static TaskAttempt getTaskAttemptFromTaskAttemptString(String attId, Task task)
      throws NotFoundException {
    TaskAttemptId attemptId;
    TaskAttempt ta;
    try {
      attemptId = TaskAttemptId.parseTaskAttemptId(attId);
    } catch (YarnException e) {
      throw new NotFoundException(e.getMessage());
    } catch (NumberFormatException ne) {
      throw new NotFoundException(ne.getMessage());
    }
    if (attemptId == null) {
      throw new NotFoundException("task attempt id " + attId
          + " not found or invalid");
    }
    ta = task.getAttempt(attemptId);
    if (ta == null) {
      throw new NotFoundException("Error getting info on task attempt id "
          + attId);
    }
    return ta;
  }


  /**
   * check for job access.
   *
   * @param job
   *          the job that is being accessed
   */
  void checkAccess(Job job, HttpServletRequest request) {
    if (!hasAccess(job, request)) {
      throw new WebApplicationException(Status.UNAUTHORIZED);
    }
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo get() {
    return getAppInfo();
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AppInfo getAppInfo() {
    return new AppInfo(this.app, this.app.context);
  }

  @GET
  @Path("/jobs")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobsInfo getJobs(@Context HttpServletRequest hsr) {
    JobsInfo allJobs = new JobsInfo();
    for (Job job : appCtx.getAllJobs().values()) {
      // getAllJobs only gives you a partial we want a full
      Job fullJob = appCtx.getJob(job.getID());
      if (fullJob == null) {
        continue;
      }
      allJobs.add(new JobInfo(fullJob, hasAccess(fullJob, hsr)));
    }
    return allJobs;
  }

  @GET
  @Path("/jobs/{jobid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobInfo getJob(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    Job job = getJobFromJobIdString(jid, appCtx);
    return new JobInfo(job, hasAccess(job, hsr));
  }

  @GET
  @Path("/jobs/{jobid}/jobattempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public AMAttemptsInfo getJobAttempts(@PathParam("jobid") String jid) {

    Job job = getJobFromJobIdString(jid, appCtx);
    AMAttemptsInfo amAttempts = new AMAttemptsInfo();
    for (AMInfo amInfo : job.getAMInfos()) {
      AMAttemptInfo attempt = new AMAttemptInfo(amInfo, job.getID().toString(),
          job.getUser());
      amAttempts.add(attempt);
    }
    return amAttempts;
  }

  @GET
  @Path("/jobs/{jobid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobCounterInfo getJobCounters(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {
    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    return new JobCounterInfo(this.appCtx, job);
  }

  @GET
  @Path("/jobs/{jobid}/conf")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public ConfInfo getJobConf(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    ConfInfo info;
    try {
      info = new ConfInfo(job, this.conf);
    } catch (IOException e) {
      throw new NotFoundException("unable to load configuration for job: "
          + jid);
    }
    return info;
  }

  @GET
  @Path("/jobs/{jobid}/tasks")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TasksInfo getJobTasks(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @QueryParam("type") String type) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    TasksInfo allTasks = new TasksInfo();
    for (Task task : job.getTasks().values()) {
      TaskType ttype = null;
      if (type != null && !type.isEmpty()) {
        try {
          ttype = TaskType.valueOf(type);
        } catch (YarnException e) {
          throw new BadRequestException("tasktype must be either m or r");
        }
      }
      if (ttype != null && task.getID().getTaskType() != ttype) {
        continue;
      }
      allTasks.add(new TaskInfo(task));
    }
    return allTasks;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskInfo getJobTask(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    return new TaskInfo(task);
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskCounterInfo getSingleTaskCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    return new JobTaskCounterInfo(task);
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptsInfo getJobTaskAttempts(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid) {
    TaskAttemptsInfo attempts = new TaskAttemptsInfo();

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);

    for (TaskAttempt ta : task.getAttempts().values()) {
      if (ta != null) {
        if (task.getID().getTaskType() == TaskType.REDUCE) {
          attempts.add(new ReduceTaskAttemptInfo(ta, task.getID().getTaskType()));
        } else {
          attempts.add(new TaskAttemptInfo(ta, task.getID().getTaskType(), true));
        }
      }
    }
    return attempts;
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public TaskAttemptInfo getJobTaskAttemptId(@Context HttpServletRequest hsr,
      @PathParam("jobid") String jid, @PathParam("taskid") String tid,
      @PathParam("attemptid") String attId) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    if (task.getID().getTaskType() == TaskType.REDUCE) {
      return new ReduceTaskAttemptInfo(ta, task.getID().getTaskType());
    } else {
      return new TaskAttemptInfo(ta, task.getID().getTaskType(), true);
    }
  }

  @GET
  @Path("/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters")
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public JobTaskAttemptCounterInfo getJobTaskAttemptIdCounters(
      @Context HttpServletRequest hsr, @PathParam("jobid") String jid,
      @PathParam("taskid") String tid, @PathParam("attemptid") String attId) {

    Job job = getJobFromJobIdString(jid, appCtx);
    checkAccess(job, hsr);
    Task task = getTaskFromTaskIdString(tid, job);
    TaskAttempt ta = getTaskAttemptFromTaskAttemptString(attId, task);
    return new JobTaskAttemptCounterInfo(ta);
  }
}
