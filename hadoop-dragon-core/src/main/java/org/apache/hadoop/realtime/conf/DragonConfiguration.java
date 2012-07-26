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
package org.apache.hadoop.realtime.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 */
public class DragonConfiguration extends Configuration {
  static {
    Configuration.addDefaultResource("dragon-default.xml");
    Configuration.addDefaultResource("dragon-site.xml");
  }

  // //////////////////////////////
  // ApplicationMaster Configs //
  // //////////////////////////////
  public static final String AM_PREFIX = "yarn.applicationmaster.";

  public static final String AM_ADDRESS = AM_PREFIX + "address";

  public static final String AM_PORT = AM_PREFIX + "port";

  /** Command line arguments passed to the DRAGON app master. */
  public static final String AM_COMMAND_OPTS = AM_PREFIX + "command-opts";
  public static final String DEFAULT_AM_COMMAND_OPTS = "-Xmx1536m";

  /** Root Logging level passed to the DRAGON app master. */
  public static final String AM_LOG_LEVEL = AM_PREFIX + "log.level";
  public static final String DEFAULT_AM_LOG_LEVEL = "INFO";

  public static final String APPLICATION_MASTER_CLASS =
      "org.apache.hadoop.realtime.server.DragonAppMaster";

  public static final String AM_LOG_LIMIT = AM_PREFIX + "log.limit.kb";

  public static final String AM_PRIORITY = AM_PREFIX + "priority";
  public static final int DEFAULT_AM_PRIORITY = 0;

  public static final String AM_MEMROY = AM_PREFIX + "memory";
  public static final int DEFAULT_AM_MEMROY = 2048;

  public static final String QUEUE_NAME = AM_PREFIX + "queue.name";

  public static final String AM_WORKING_DIR = AM_PREFIX +"working-dir";

  public static final String DEFAULT_AM_WORKING_DIR = "hadoop-yarn/working";

  public DragonConfiguration() {
    super();
  }

  public DragonConfiguration(Configuration conf) {
    super(conf);
    if (!(conf instanceof DragonConfiguration)) {
      this.reloadConfiguration();
    }
  }

  public DragonConfiguration(String conf) {
    this(new Path(conf));
  }

  public DragonConfiguration(Path conf) {
    super();
    addResource(conf);
  }
}
