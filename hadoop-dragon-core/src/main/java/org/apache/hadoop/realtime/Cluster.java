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
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Provides a way to access information about the dragon cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Cluster {
  
  @InterfaceStability.Evolving
  public static enum JobTrackerStatus {INITIALIZING, RUNNING};

  private UserGroupInformation ugi;
  private DragonJobService client;
  private Configuration conf;
  private FileSystem fs = null;
  private Path sysDir = null;
  private Path stagingAreaDir = null;
  private Path jobHistoryDir = null;
  private static final Log LOG = LogFactory.getLog(Cluster.class);

  public Cluster(Configuration conf) 
      throws IOException {
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    initialize(conf);
  }
  
  private void initialize(Configuration conf)
      throws IOException {
    // TODO: initialize client serivce here
  }

  DragonJobService getClient() {
    return client;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /**
   * Close the <code>Cluster</code>.
   */
  public synchronized void close() throws IOException {

  }

  /**
   * Get the file system where job-specific files are stored
   * 
   * @return object of FileSystem
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized FileSystem getFileSystem() 
      throws IOException, InterruptedException {
    if (this.fs == null) {
      try {
        this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException, InterruptedException {
            final Path sysDir = new Path(client.getSystemDir());
            return sysDir.getFileSystem(getConf());
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return fs;
  }



}
