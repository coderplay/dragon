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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class JobSubmitter {
  protected static final Log LOG = LogFactory.getLog(JobSubmitter.class);
  private FileSystem submitFs;
  private String submitHostName;
  private String submitHostAddress;
  
  DragonJobService client;
  
  JobSubmitter(FileSystem submitFs, DragonJobService client)
      throws IOException {
    this.submitFs = submitFs;
    this.client = client;
  }

  /**
   * Internal method for submitting jobs to the system.
   * 
   * <p>The job submission process involves:
   * <ol>
   *   <li>
   *   Checking the input and output specifications of the job.
   *   </li>
   *   <li>
   *   Serializing the job description file for the job.
   *   </li>
   *   <li>
   *   Setup the requisite accounting information for the 
   *   DistributedCache of the job, if necessary.
   *   </li>
   *   <li>
   *   Copying the job's jar and configuration to the dragon system
   *   directory on the distributed file-system. 
   *   </li>
   *   <li>
   *   Submitting the job to the {@link DragonJobService} and optionally
   *   monitoring it's status.
   *   </li>
   * </ol></p>
   * @param job the configuration to submit
   * @param cluster the handle to the Cluster
   * @throws ClassNotFoundException
   * @throws InterruptedException
   * @throws IOException
   */
  boolean submitJobInternal(DragonJob job, Cluster cluster) 
      throws ClassNotFoundException, InterruptedException, IOException {
    return false;
  }
  
  private void writeConf(Configuration conf, Path jobFile) throws IOException {
    // Write job file to JobTracker's fs
    FSDataOutputStream out =
        FileSystem.create(submitFs, jobFile, new FsPermission(
            JobSubmissionFiles.JOB_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }


}
