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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.realtime.conf.DragonConfiguration;
import org.apache.hadoop.realtime.job.JobPriority;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.realtime.records.JobReport;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Evolving
/**
 * Dragon command line interface that interprets the dragon job options
 */
public final class DragonCLI extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(DragonCLI.class);

  private String getJobPriorityNames() {
    StringBuffer sb = new StringBuffer();
    for (JobPriority p : JobPriority.values()) {
      sb.append(p.name()).append(" ");
    }
    return sb.substring(0, sb.length()-1);
  }

  /**
   * Display usage of the command-line tool and terminate execution.
   */
  private void displayUsage(String cmd) {
    String prefix = "Usage: CLI ";
    String jobPriorityValues = getJobPriorityNames();
    String taskStates = "running, completed";
    if ("-submit".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-file>]");
    } else if ("-status".equals(cmd) || "-kill".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id>]");
    } else if ("-counter".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <group-name> <counter-name>]");
    } else if ("-events".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <from-event-#> <#-of-events>]. Event #s start from 1.");
    } else if ("-history".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <jobHistoryFile>]");
    } else if ("-list".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " [all]]");
    } else if ("-kill-task".equals(cmd) || "-fail-task".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <task-attempt-id>]");
    } else if ("-set-priority".equals(cmd)) {
      System.err.println(prefix + "[" + cmd + " <job-id> <priority>]. " +
          "Valid values for priorities are: "
          + jobPriorityValues);
    } else if ("-list-attempt-ids".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <task-type> <task-state>]. " +
          "Valid values for <task-state> are " + taskStates);
    } else if ("-logs".equals(cmd)) {
      System.err.println(prefix + "[" + cmd +
          " <job-id> <task-attempt-id>]. " +
          " <task-attempt-id> is optional to get task attempt logs.");
    } else {
      System.err.printf(prefix + "<command> <args>\n");
      System.err.printf("\t[-submit <job-file>]\n");
      System.err.printf("\t[-status <job-id>]\n");
      System.err.printf("\t[-counter <job-id> <group-name> <counter-name>]\n");
      System.err.printf("\t[-kill <job-id>]\n");
      System.err.printf("\t[-set-priority <job-id> <priority>]. " +
          "Valid values for priorities are: " + jobPriorityValues + "\n");
      System.err.printf("\t[-events <job-id> <from-event-#> <#-of-events>]\n");
      System.err.printf("\t[-history <jobHistoryFile>]\n");
      System.err.printf("\t[-list [all]]\n");
      System.err.println("\t[-list-attempt-ids <job-id> <task-type> " +
          "<task-state>]. " +
          "Valid values for <task-state> are " + taskStates);
      System.err.printf("\t[-kill-task <task-attempt-id>]\n");
      System.err.printf("\t[-fail-task <task-attempt-id>]\n");
      System.err.printf("\t[-logs <job-id> <task-attempt-id>]\n\n");
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /** Construct a JobId object from given string */
  private JobId jobIdForName(String name) throws IllegalArgumentException {
    if (name == null)
      return null;
    try {
      String[] parts = name.split("_");
      if (parts.length == 3) {
        if (parts[0].equals("job")) {
          long clusterTimeStamp = Long.parseLong(parts[1]);
          int id = Integer.parseInt(parts[2]);
          JobId jobId = Records.newRecord(JobId.class);
          jobId.setId(id);
          ApplicationId appId = Records.newRecord(ApplicationId.class);
          appId.setId(id);
          appId.setClusterTimestamp(clusterTimeStamp);
          jobId.setAppId(appId);
          return jobId;
        }
      }
    } catch (Exception ex) {// fall below
    }
    throw new IllegalArgumentException("JobId string : " + name
        + " is not properly formed");
  }

  @Override
  public int run(String[] args) throws Exception {
    int exitCode = -1;
    if (args.length < 1) {
      displayUsage("");
      return exitCode;
    }
    // process arguments
    String cmd = args[0];
    String submitJobFile = null;
    String jobid = null;
    String taskid = null;
    String historyFile = null;
    String counterGroupName = null;
    String counterName = null;
    JobPriority jp = null;
    String taskState = null;
    int fromEvent = 0;
    int nEvents = 0;
    boolean getStatus = false;
    boolean getCounter = false;
    boolean killJob = false;
    boolean listEvents = false;
    boolean viewHistory = false;
    boolean viewAllHistory = false;
    boolean listJobs = false;
    boolean listAllJobs = false;
    boolean displayTasks = false;
    boolean killTask = false;
    boolean failTask = false;
    boolean setJobPriority = false;
    boolean logs = false;

    if ("-submit".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      submitJobFile = args[1];
    } else if ("-status".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      getStatus = true;
    } else if("-counter".equals(cmd)) {
      if (args.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      getCounter = true;
      jobid = args[1];
      counterGroupName = args[2];
      counterName = args[3];
    } else if ("-kill".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      killJob = true;
    } else if ("-set-priority".equals(cmd)) {
      if (args.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      try {
        jp = JobPriority.valueOf(args[2]);
      } catch (IllegalArgumentException iae) {
        LOG.info(iae);
        displayUsage(cmd);
        return exitCode;
      }
      setJobPriority = true;
    } else if ("-events".equals(cmd)) {
      if (args.length != 4) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      fromEvent = Integer.parseInt(args[2]);
      nEvents = Integer.parseInt(args[3]);
      listEvents = true;
    } else if ("-history".equals(cmd)) {
      if (args.length != 2 && !(args.length == 3 && "all".equals(args[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      viewHistory = true;
      if (args.length == 3 && "all".equals(args[1])) {
        viewAllHistory = true;
        historyFile = args[2];
      } else {
        historyFile = args[1];
      }
    } else if ("-list".equals(cmd)) {
      if (args.length != 1 && !(args.length == 2 && "all".equals(args[1]))) {
        displayUsage(cmd);
        return exitCode;
      }
      if (args.length == 2 && "all".equals(args[1])) {
        listAllJobs = true;
      } else {
        listJobs = true;
      }
    } else if("-kill-task".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      killTask = true;
      taskid = args[1];
    } else if("-fail-task".equals(cmd)) {
      if (args.length != 2) {
        displayUsage(cmd);
        return exitCode;
      }
      failTask = true;
      taskid = args[1];
    } else if ("-list-attempt-ids".equals(cmd)) {
      if (args.length != 3) {
        displayUsage(cmd);
        return exitCode;
      }
      jobid = args[1];
      taskState = args[2];
      displayTasks = true;
    } else if ("-logs".equals(cmd)) {
      if (args.length == 2 || args.length ==3) {
        logs = true;
        jobid = args[1];
        if (args.length == 3) {
          taskid = args[2];
        }  else {
          taskid = null;
        }
      } else {
        displayUsage(cmd);
        return exitCode;
      }
    } else {
      displayUsage(cmd);
      return exitCode;
    }

    // initialize cluster
    DragonJobServiceFactory factory = new DragonJobServiceFactory();
    DragonJobService service = factory.create(getConf());

    // Submit the request
    try {
      if (submitJobFile != null) {
        DragonJob job =
            DragonJob.getInstance(new DragonConfiguration(submitJobFile));
        job.submit();
        System.out.println("Created job " + job.getID());
        exitCode = 0;
      } else if (getStatus) {
        JobId jobId = jobIdForName(jobid);
        JobReport report = service.getJobReport(jobId);
        if (report == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          exitCode = 0;
        }
      } else if (getCounter) {

      } else if (killJob) {
        JobId jobId = jobIdForName(jobid);
        JobReport report = service.getJobReport(jobId);
        if (report == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          service.killJob(jobId);
          System.out.println("Killed job " + jobid);
          exitCode = 0;
        }
      } else if (setJobPriority) {
        JobId jobId = jobIdForName(jobid);
        JobReport report = service.getJobReport(jobId);
        if (report == null) {
          System.out.println("Could not find job " + jobid);
        } else {
          System.out.println("Changed job priority.");
          exitCode = 0;
        }
      } else if (viewHistory) {
        exitCode = 0;
      } else if (listEvents) {
        exitCode = 0;
      } else if (listJobs) {
        exitCode = 0;
      } else if (listAllJobs) {
        exitCode = 0;
      } else if (displayTasks) {

      } else if(killTask) {
        final TaskAttemptId taskId = TaskAttemptId.forName(taskid);
        service.killTask(taskId, false);
        exitCode = 0;
      } else if(failTask) {
        final TaskAttemptId taskId = TaskAttemptId.forName(taskid);
        service.killTask(taskId, true);
        exitCode = 0;
      } else if (logs) {
      }
    } catch (RemoteException re) {
      IOException unwrappedException = re.unwrapRemoteException();
      if (unwrappedException instanceof AccessControlException) {
        System.out.println(unwrappedException.getMessage());
      } else {
        throw re;
      }
    } finally {
    }
    return exitCode;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new DragonCLI(), args);
    System.exit(res);
  }
}