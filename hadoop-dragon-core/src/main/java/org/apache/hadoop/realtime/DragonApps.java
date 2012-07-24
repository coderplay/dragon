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

package org.apache.hadoop.realtime;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.job.TaskLog;
import org.apache.hadoop.realtime.job.TaskLog.LogName;
import org.apache.hadoop.realtime.records.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import static org.apache.hadoop.yarn.util.StringHelper._join;
import static org.apache.hadoop.yarn.util.StringHelper._split;

/**
 * Helper class for Dragon applications
 */
@Private
@Unstable
public class DragonApps extends Apps {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  public static final String JOB = "job";
  public static final String TASK = "task";
  public static final String ATTEMPT = "attempt";

  public static void setClasspath(Map<String, String> environment,
      Configuration conf) {
    // Add standard Hadoop classes
    for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH)
        .split(",")) {
      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c.trim());
    }
    // Add Job_Jar
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        DragonJobConfig.JOB_JAR);
    // Add current path
    Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
        Environment.PWD.$() + Path.SEPARATOR + "*");
  }

  /**
   * 
   * @param applicationId
   * @param conf
   * @param amContainer
   * @return
   * @throws IOException
   */
  public static ApplicationSubmissionContext newApplicationSubmissionContext(
      ApplicationId applicationId, Configuration conf,
      ContainerLaunchContext amContainer) throws IOException {
    ApplicationSubmissionContext appContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId);
    appContext
        .setUser(UserGroupInformation.getCurrentUser().getShortUserName());
    appContext.setQueue(conf.get(DragonJobConfig.QUEUE_NAME,
        YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName(conf.get(DragonJobConfig.JOB_NAME,
        YarnConfiguration.DEFAULT_APPLICATION_NAME));
    appContext.setAMContainerSpec(amContainer);
    return appContext;
  }

  /**
   * 
   * @param conf
   * @return
   */
  public static Map<ApplicationAccessType, String>
      setupACLs(Configuration conf) {
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, conf.get(
        DragonJobConfig.JOB_ACL_VIEW_JOB,
        DragonJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
    acls.put(ApplicationAccessType.MODIFY_APP, conf.get(
        DragonJobConfig.JOB_ACL_MODIFY_JOB,
        DragonJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));
    return acls;
  }

  /**
   * 
   * @param fs
   * @param p
   * @return
   * @throws IOException
   */
  public static LocalResource createApplicationResource(FileSystem fs, Path p)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs.resolvePath(rsrcStat
        .getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  /**
   * Setup the memory ApplicationMaster used.
   * 
   * @param conf
   * @return
   */
  public static Resource setupResources(Configuration conf) {
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(conf.getInt(DragonJobConfig.DRAGON_AM_VMEM_MB,
        DragonJobConfig.DEFAULT_DRAGON_AM_VMEM_MB));
    return capability;
  }

  /**
   * Add the JVM system properties necessary to configure
   * {@link ContainerLogAppender}.
   * 
   * @param logLevel
   * @param logSize
   * @param vargs
   */
  public static void addLog4jSystemProperties(String logLevel, long logSize,
      Vector<CharSequence> vargs) {
    vargs.add("-Dlog4j.configuration=dragon-container-log4j.properties");
    vargs.add("-D" + DragonJobConfig.TASK_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + DragonJobConfig.TASK_LOG_SIZE + "=" + logSize);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA");
  }

  private static final String STAGING_CONSTANT = ".staging";

  public static Path getStagingAreaDir(Configuration conf, String user) {
    return new Path(conf.get(DragonJobConfig.DRAGON_AM_STAGING_DIR,
        DragonJobConfig.DEFAULT_DRAGON_AM_STAGING_DIR)
        + Path.SEPARATOR
        + user
        + Path.SEPARATOR + STAGING_CONSTANT);
  }

  public static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
        + filter.toString();
  }

  public static String toString(JobId jid) {
    return _join(JOB, jid.getAppId().getClusterTimestamp(), jid.getAppId().getId(), jid.getId());
  }

  public static JobId toJobID(String jid) {
    Iterator<String> it = _split(jid).iterator();
    return toJobID(JOB, jid, it);
  }

  // mostly useful for parsing task/attempt id like strings
  public static JobId toJobID(String prefix, String s, Iterator<String> it) {
    ApplicationId appId = toAppID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    JobId jobId = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(JobId.class);
    jobId.setAppId(appId);
    jobId.setId(Integer.parseInt(it.next()));
    return jobId;
  }

  public static String toString(TaskId tid) {
    return _join("task", tid.getJobId().getAppId().getClusterTimestamp(),
        tid.getJobId().getAppId().getId(), tid.getJobId().getId(),
        taskSymbol(tid.getTaskType()), tid.getId());
  }

  public static TaskId toTaskID(String tid) {
    Iterator<String> it = _split(tid).iterator();
    return toTaskID(TASK, tid, it);
  }

  public static TaskId toTaskID(String prefix, String s, Iterator<String> it) {
    JobId jid = toJobID(prefix, s, it);
    shouldHaveNext(prefix, s, it);
    TaskId tid = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(TaskId.class);
    tid.setJobId(jid);
    tid.setTaskType(taskType(it.next()));
    shouldHaveNext(prefix, s, it);
    tid.setId(Integer.parseInt(it.next()));
    return tid;
  }

  public static String toString(TaskAttemptId taid) {
    return _join("attempt",
        taid.getTaskId().getJobId().getAppId().getClusterTimestamp(),
        taid.getTaskId().getJobId().getAppId().getId(),
        taid.getTaskId().getJobId().getId(),
        taskSymbol(taid.getTaskId().getTaskType()),
        taid.getTaskId().getId(),
        taid.getId());
  }

  public static String taskSymbol(TaskType type) {
    switch (type) {
      case MAP:           return "m";
      case REDUCE:        return "r";
    }
    throw new YarnException("Unknown task type: "+ type.toString());
  }

  public static TaskType taskType(String symbol) {
    // JDK 7 supports switch on strings
    if (symbol.equals("m")) return TaskType.MAP;
    if (symbol.equals("r")) return TaskType.REDUCE;
    throw new YarnException("Unknown task symbol: "+ symbol);
  }

  public static TaskAttemptId toTaskAttemptID(String taid) {
    Iterator<String> it = _split(taid).iterator();
    TaskId tid = toTaskID(ATTEMPT, taid, it);
    shouldHaveNext(ATTEMPT, taid, it);
    TaskAttemptId taId = RecordFactoryProvider.
        getRecordFactory(null).newRecordInstance(TaskAttemptId.class);
    taId.setTaskId(tid);
    taId.setId(Integer.parseInt(it.next()));
    return taId;
  }


  public static enum TaskAttemptStateUI {
    NEW(
        new TaskAttemptState[] { TaskAttemptState.NEW,
            TaskAttemptState.UNASSIGNED, TaskAttemptState.ASSIGNED }),
    RUNNING(
        new TaskAttemptState[] { TaskAttemptState.RUNNING,
            TaskAttemptState.SUCCESS_CONTAINER_CLEANUP,
            TaskAttemptState.FAIL_CONTAINER_CLEANUP,
            TaskAttemptState.FAIL_TASK_CLEANUP,
            TaskAttemptState.KILL_CONTAINER_CLEANUP,
            TaskAttemptState.KILL_TASK_CLEANUP }),
    FAILED(new TaskAttemptState[] { TaskAttemptState.FAILED}),
    KILLED(new TaskAttemptState[] { TaskAttemptState.KILLED});

    private final List<TaskAttemptState> correspondingStates;

    private TaskAttemptStateUI(TaskAttemptState[] correspondingStates) {
      this.correspondingStates = Arrays.asList(correspondingStates);
    }

    public boolean correspondsTo(TaskAttemptState state) {
      return this.correspondingStates.contains(state);
    }
  }

}
