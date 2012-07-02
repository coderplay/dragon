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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.job.TaskLog;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class DragonChildJVM {

  private static String getChildLogLevel(Configuration conf) {
    return conf.get(DragonJobConfig.TASK_LOG_LEVEL,
        DragonJobConfig.DEFAULT_LOG_LEVEL);
  }

  public static void setVMEnv(Map<String, String> environment,
      Configuration conf) {  
    environment.put("HADOOP_ROOT_LOGGER", getChildLogLevel(conf) + ",CLA");
    // Add stdout/stderr env
    environment.put(DragonJobConfig.STDOUT_LOGFILE_ENV,
        DragonApps.getTaskLogFile(TaskLog.LogName.STDOUT));
    environment.put(DragonJobConfig.STDERR_LOGFILE_ENV,
        DragonApps.getTaskLogFile(TaskLog.LogName.STDERR));
    environment.put(DragonJobConfig.APPLICATION_ATTEMPT_ID_ENV,
        conf.get(DragonJobConfig.APPLICATION_ATTEMPT_ID).toString());
  }

  private static String getChildJavaOpts(Configuration conf) {
    String userClasspath = "";
    String adminClasspath = "";
    userClasspath =
        conf.get(DragonJobConfig.DRAGON_TASK_JAVA_OPTS,
            DragonJobConfig.DEFAULT_DRAGON_TASK_JAVA_OPTS);
    adminClasspath =
        conf.get(DragonJobConfig.DRAGON_ADMIN_JAVA_OPTS,
            DragonJobConfig.DEFAULT_DRAGON_ADMIN_JAVA_OPTS);

    return adminClasspath + " " + userClasspath;
  }

  private static void setupLog4jProperties(Configuration conf,
      Vector<CharSequence> vargs,long logSize) {
    String logLevel = getChildLogLevel(conf);
    DragonApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  public static List<String> getVMCommand(
      InetSocketAddress childServiceAddr, TaskAttemptId attemptId,
     Configuration conf, ContainerId containerId) {
    
    Vector<CharSequence> vargs = new Vector<CharSequence>(8);

    vargs.add("exec");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    String javaOpts = getChildJavaOpts(conf);
    javaOpts = javaOpts.replace("@taskid@", attemptId.toString());
    String[] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    Path childTmpDir =
        new Path(Environment.PWD.$(),
            YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // Setup the log4j prop
    long logSize = TaskLog.getTaskLogLength(conf);
    setupLog4jProperties(conf, vargs,logSize);

    // Add main class and its arguments
    vargs.add(DragonChild.class.getName()); // main of Child
    // pass TaskAttemptListener's address
    vargs.add(childServiceAddr.getAddress().getHostAddress());
    vargs.add(Integer.toString(childServiceAddr.getPort()));
    // pass job identifier
    vargs.add(String.valueOf(attemptId.getTaskId().getJobId().toString())); 
    // Finally add the container id
    vargs.add(String.valueOf(containerId.toString()));

    vargs.add("1>" + DragonApps.getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + DragonApps.getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }
}
