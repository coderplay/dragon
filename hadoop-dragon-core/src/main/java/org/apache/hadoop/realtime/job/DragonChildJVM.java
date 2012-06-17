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

package org.apache.hadoop.realtime.job;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.job.TaskLog.LogName;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

@SuppressWarnings("deprecation")
public class DragonChildJVM {

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR
        + filter.toString();
  }

  private static String getChildEnv(Configuration conf) {
    return conf.get(DragonJobConfig.DRAGON_ADMIN_USER_ENV,
        DragonJobConfig.DEFAULT_DRAGON_ADMIN_USER_ENV);
  }

  private static String getChildLogLevel(Configuration conf) {

    return conf.get(DragonJobConfig.TASK_LOG_LEVEL,
        DragonJobConfig.DEFAULT_LOG_LEVEL.toString());
  }

  public static void setVMEnv(Map<String, String> environment,
      Configuration conf) {
    String dragonChildEnv = getChildEnv(conf);
    Apps.setEnvFromInputString(environment, dragonChildEnv);
    environment.put("HADOOP_ROOT_LOGGER", getChildLogLevel(conf) + ",CLA");

    // TODO: The following is useful for instance in streaming tasks. Should be
    // set in ApplicationMaster's env by the RM.
    String hadoopClientOpts = System.getenv("HADOOP_CLIENT_OPTS");
    if (hadoopClientOpts == null) {
      hadoopClientOpts = "";
    } else {
      hadoopClientOpts = hadoopClientOpts + " ";
    }
    // FIXME: don't think this is also needed given we already set java
    // properties.
    long logSize = TaskLog.getTaskLogLength(conf);
    Vector<CharSequence> logProps = new Vector<CharSequence>(4);
    //setupLog4jProperties(conf, logProps, logSize);
    Iterator<CharSequence> it = logProps.iterator();
    StringBuffer buffer = new StringBuffer();
    while (it.hasNext()) {
      buffer.append(" " + it.next());
    }
    hadoopClientOpts = hadoopClientOpts + buffer.toString();
    environment.put("HADOOP_CLIENT_OPTS", hadoopClientOpts);

    // Add stdout/stderr env
    environment.put(DragonJobConfig.STDOUT_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDOUT));
    environment.put(DragonJobConfig.STDERR_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDERR));
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

    // Add admin classpath first so it can be overridden by user.
    return adminClasspath + " " + userClasspath;
  }

  private static void setupLog4jProperties(Configuration conf,
      Vector<CharSequence> vargs,long logSize) {
    String logLevel = getChildLogLevel(conf);
    DragonApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, TaskAttemptId attemptId,
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
    //setupLog4jProperties(conf, vargs,logSize);

    // Add main class and its arguments
    vargs.add(YarnChild.class.getName()); // main of Child
    // pass TaskAttemptListener's address
    vargs.add(taskAttemptListenerAddr.getAddress().getHostAddress());
    vargs.add(Integer.toString(taskAttemptListenerAddr.getPort()));
    vargs.add(String.valueOf(attemptId.getTaskId().getJobId().toString())); // pass task identifier
    vargs.add(String.valueOf(containerId.toString()));

    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

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
