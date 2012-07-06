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
import org.apache.hadoop.realtime.DragonJobConfig;
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
  public static final String JOB_HISTORY_FILE_EXTENSION = ".jhist";

  public static Path getJobHistoryFile(Path logDir, JobId jobId, int attempt) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(
          logDir, jobId.toString() + "_" + attempt + JOB_HISTORY_FILE_EXTENSION);
    }
    return jobFilePath;
  }

  public static String getConfiguredHistoryStagingDirPrefix(Configuration conf) throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();

    Path path = new Path(
        conf.get(DragonJobConfig.JOB_HISTORY_DIR) +
            Path.SEPARATOR + user + Path.SEPARATOR);
    String logDir = path.toString();
    return logDir;
  }
}
