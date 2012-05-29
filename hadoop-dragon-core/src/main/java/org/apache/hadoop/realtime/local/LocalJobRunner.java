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
package org.apache.hadoop.realtime.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonConfig;
import org.apache.hadoop.realtime.DragonJob;
import org.apache.hadoop.realtime.DragonJobService;
import org.apache.hadoop.realtime.job.Job;
import org.apache.hadoop.realtime.job.Task;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskId;
import org.apache.hadoop.realtime.records.TaskReport;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalJobRunner implements DragonJobService {
  private static final Log LOG = LogFactory.getLog(LocalJobRunner.class);

  /** The maximum number of map tasks to run in parallel in LocalJobRunner */
  public static final String LOCAL_MAX_TASKS = "dragon.local.tasks.maximum";
  private static final String jobDir = "localRunner/";

  private Configuration conf;
  private JobId jobId;
  private ApplicationId appId;
  private LocalJob job;
  private LocalJobRunnerMetrics localMetrics;

  private FileSystem fs;
  final Random rand = new Random();

  public LocalJobRunner(Configuration conf) throws IOException {
    this.conf = conf;
    this.fs = FileSystem.getLocal(conf);
  }

  @Override
  public JobId getNewJobId() throws IOException, InterruptedException {
    jobId = new JobId(appId);
    return jobId;
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir =
        new Path(conf.get(DragonConfig.SYSTEM_DIR, "/tmp/hadoop/dragon/system"));
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    Path stagingRootDir =
        new Path(conf.get(DragonConfig.STAGING_AREA_ROOT,
            "/tmp/hadoop/mapred/staging"));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user;
    if (ugi != null) {
      user = ugi.getShortUserName() + rand.nextInt();
    } else {
      user = "dummy" + rand.nextInt();
    }
    return fs.makeQualified(new Path(stagingRootDir, user + "/.staging"))
        .toString();
  }

  protected class JobRunnable implements Runnable {
    private LocalJob job;

    public JobRunnable(LocalJob job) {
      this.job = job;
    }

    @Override
    public void run() {
      try {
        List<TaskRunnable> taskRunnables = getTaskRunnables(job, jobId);
        ExecutorService taskService = createTaskExecutor(taskRunnables.size());
        // Start populating the executor with work units.
        // They may begin running immediately (in other threads).
        for (Runnable r : taskRunnables) {
          taskService.submit(r);
        }
        try {
          taskService.shutdown(); // Instructs queue to drain.
          // Wait for tasks to finish; do not use a time-based timeout.
          // (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179024)
          LOG.info("Waiting for tasks");
          taskService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
          // Cancel all threads.
          taskService.shutdownNow();
          throw ie;
        }
        LOG.info("Task executor complete.");
        for (TaskRunnable r : taskRunnables) {
          if (r.storedException != null) {
            throw new Exception(r.storedException);
          }
        }
      } catch (Throwable t) {
        LOG.warn(jobId, t);
      } finally {
        try {
          // TODO: clean workDir
        } catch (Exception e) {
          LOG.warn("Error cleaning up " + jobId + ": " + e);
        }
      }
    }
  }

  /**
   * Create Runnables to encapsulate tasks for use by the executor service.
   * 
   * @param jobId the job id
   * @param mapOutputFiles a mapping from task attempts to output files
   * @return a List of Runnables, one per task.
   */
  protected List<TaskRunnable> getTaskRunnables(LocalJob job, JobId jobId) {
    int numTasks = 0;
    ArrayList<TaskRunnable> list = new ArrayList<TaskRunnable>();
    for (Task task : job.getTasks()) {
      list.add(new TaskRunnable((LocalTask)task, numTasks++, jobId));
    }
    return list;
  }

  /**
   * Creates the executor service used to run tasks.
   * 
   * @param numTasks the total number of map tasks to be run
   * @return an ExecutorService instance that handles map tasks
   */
  protected ExecutorService createTaskExecutor(int numTasks) {
    // Determine the size of the thread pool to use
    int maxThreads = conf.getInt(LOCAL_MAX_TASKS, 1);
    if (maxThreads < 1) {
      throw new IllegalArgumentException("Configured " + LOCAL_MAX_TASKS
          + " must be >= 1");
    }
    maxThreads = Math.min(maxThreads, numTasks);
    maxThreads = Math.max(maxThreads, 1); // In case of no tasks.

    LOG.debug("Starting thread pool executor.");
    LOG.debug("Max local threads: " + maxThreads);
    LOG.debug("Tasks to process: " + numTasks);

    // Create a new executor service to drain the work queue.
    ThreadFactory tf =
        new ThreadFactoryBuilder().setNameFormat(
            "LocalJobRunner Task Executor #%d").build();
    ExecutorService executor = Executors.newFixedThreadPool(maxThreads, tf);
    return executor;
  }

  /**
   * A Runnable instance that handles a map task to be run by an executor.
   */
  protected class TaskRunnable implements Runnable {
    private final LocalTask task;
    private final int taskId;
    private final JobId jobId;
    private final Configuration localConf;

    public volatile Throwable storedException;

    public TaskRunnable(LocalTask task, int taskId, JobId jobId) {
      this.task = task;
      this.taskId = taskId;
      this.jobId = jobId;
      this.localConf = new Configuration(conf);
    }

    public void run() {
      try {
        TaskAttemptId attemptId =
            new TaskAttemptId(new TaskId(jobId, taskId), 0);
        LOG.info("Starting task: " + taskId);
        setupChildMapredLocalDirs(task, localConf);
        localMetrics.launchTask(attemptId);
        task.run();
        localMetrics.completeTask(attemptId);
        LOG.info("Finishing task: " + taskId);
      } catch (Throwable e) {
        this.storedException = e;
      }
    }

  }

  void setupChildMapredLocalDirs(Task t, Configuration conf) {
    String[] localDirs = conf.getTrimmedStrings(DragonConfig.LOCAL_DIR);
    String jobId = t.getId().getJobId().toString();
    String taskId = t.getId().toString();
    String user = job.getUser();
    StringBuffer childMapredLocalDir =
        new StringBuffer(localDirs[0] + Path.SEPARATOR
            + getLocalTaskDir(user, jobId, taskId));
    for (int i = 1; i < localDirs.length; i++) {
      childMapredLocalDir.append("," + localDirs[i] + Path.SEPARATOR
          + getLocalTaskDir(user, jobId, taskId));
    }
    LOG.debug(DragonConfig.LOCAL_DIR + " for child : " + childMapredLocalDir);
    conf.set(DragonConfig.LOCAL_DIR, childMapredLocalDir.toString());
  }

  static final String SUBDIR = jobDir;
  static final String JOBCACHE = "jobcache";

  static String getLocalTaskDir(String user, String jobid, String taskid) {
    String taskDir =
        SUBDIR + Path.SEPARATOR + user + Path.SEPARATOR + JOBCACHE
            + Path.SEPARATOR + jobid + Path.SEPARATOR + taskid;
    return taskDir;
  }

  @Override
  public AccessControlList getQueueAdmins(String queueName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void killJob(JobId jobid) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean killTask(TaskAttemptId taskId, boolean shouldFail)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<TaskReport> getTaskReports(JobId jobid) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobReport getJobReport(JobId jobId) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean submitJob(DragonJob dragonJob) throws IOException,
      InterruptedException {
    job = new LocalJob(conf);
    job.setCredentials(dragonJob.getCredentials());
    return false;
  }

}
