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
package org.apache.hadoop.realtime.jobhistory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.realtime.DragonApps;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.JobSubmissionFiles;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * class description goes here.
 */
public class JobHistoryUtils {

  public static final short HISTORY_DIR_PERMISSIONS = 0700;

  /**
   * Job History File extension.
   */
  public static final String JOB_HISTORY_FILE = "job.jhist";

  /**
   * Suffix for configuration files.
   */
  public static final String CONF_FILE_NAME = "job.xml";
  private static final String DESCR_FILE_NAME = "job.desc";

  public static String getConfiguredHistoryDirPrefix(Configuration conf) throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    Path path = new Path(
        conf.get(DragonJobConfig.JOB_HISTORY_DIR, DragonJobConfig.DEFAULT_JOB_HISTORY_DIR) +
            Path.SEPARATOR + user + Path.SEPARATOR);
    String historyDir = path.toString();
    return historyDir;
  }

  /**
   * Gets the configured directory prefix for In Progress history files.
   * @param conf
   * @return A string representation of the prefix.
   */
  public static String getConfiguredStagingDirPrefix(Configuration conf)
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path path = DragonApps.getStagingAreaDir(conf, user);
    String logDir = path.toString();
    return logDir;
  }

  public static Path getJobHistoryFile(Path historyDir, JobId jobId, int attempt) {
    Path jobFilePath = null;
    if (historyDir != null) {
      jobFilePath = new Path(
          historyDir,
          jobId.toString() + Path.SEPARATOR +
              attempt + Path.SEPARATOR + JOB_HISTORY_FILE);
    }

    return jobFilePath;
  }

  public static Path getHistoryConfFile(Path historyDir, JobId jobId, int attemptId) {
    Path jobFilePath = null;
    if (historyDir != null) {
      jobFilePath = new Path(historyDir, jobId.toString()  +
          Path.SEPARATOR + attemptId + Path.SEPARATOR + CONF_FILE_NAME);
    }
    return jobFilePath;
  }

  public static Path getHistoryJobDescriptionFile(
      Path historyDir, JobId jobId, int attemptId) {
    Path jobFilePath = null;
    if (historyDir != null) {
      jobFilePath = new Path(historyDir, jobId.toString()  +
         Path.SEPARATOR + attemptId+ Path.SEPARATOR + DESCR_FILE_NAME);
    }
    return jobFilePath;
  }

  public static Path getStagingJobDescriptionFile(Path stagingDirPath, JobId jobId) {
    Path submitJobDir = new Path(stagingDirPath, jobId.toString());
    return JobSubmissionFiles.getJobDescriptionFile(submitJobDir);
  }
}
