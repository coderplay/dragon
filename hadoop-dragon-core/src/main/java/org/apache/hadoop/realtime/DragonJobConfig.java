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

public class DragonJobConfig {
  
  public static final String ID = "dragon.job.id";

  public static final String JOB_NAME = "dragon.job.name";
  
  // This should be the name of the localized job-jar file on the node running
  // individual containers/tasks.
  public static final String JOB_JAR = "graphjob.jar";

  // This should be the directory where splits file gets localized on the node
  // running ApplicationMaster.
  public static final String JOB_SUBMIT_DIR = "graphJobSubmitDir";

  public static final String JOB_SUBMIT_FILE_REPLICATION = 
      "dragon.client.submit.file.replication";

  public static final String CHILD_JAVA_OPTS =
      "dragon.child.java.opts";
  
  // This should be the name of the localized job-configuration file on the node
  // running ApplicationMaster and Task
  public static final String JOB_CONF_FILE = "graphjob.xml";

  public static final String JOB_DESCRIPTION_FILE = "graphjob.desc";


  /* Entry class for child processes on each node */
  public static final String CHILD_CLASS =
      "org.apache.hadoop.realtime.DragonChild";
  
  public static final String JOB_SUBMITHOST =
      "dragon.job.submithostname";
  public static final String JOB_SUBMITHOSTADDR =
      "dragon.job.submithostaddress";

  public static final String JOB_NAMENODES = "dragon.job.hdfs-servers";

  /*
   * config for tracking the local file where all the credentials for the job
   * credentials.
   */
  public static final String DRAGON_JOB_CREDENTIALS_BINARY =
      "dragon.job.credentials.binary";
  
  public static final String DRAGON_JOB_CREDENTIALS_JSON =
      "dragon.job.credentials.json";
}
