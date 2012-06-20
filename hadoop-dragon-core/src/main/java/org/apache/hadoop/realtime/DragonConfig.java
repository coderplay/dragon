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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Place holder for cluster level configuration keys.
 * 
 * The keys should have "mapreduce.cluster." as the prefix. 
 *
 */
@InterfaceAudience.Private
public interface DragonConfig {
  // Cluster-level configuration parameters
  public static final String TEMP_DIR = "dragon.cluster.temp.dir";
  public static final String LOCAL_DIR = "dragon.cluster.local.dir";
  
  public static final String LOCAL_CACHE_SIZE = 
      "dragon.cache.local.size";
  public static final String LOCAL_CACHE_SUBDIRS_LIMIT =
      "dragon.cache.local.numberdirectories";
  /**
   * Percentage of the local distributed cache that should be kept in between
   * garbage collection.
   */
  public static final String LOCAL_CACHE_KEEP_AROUND_PCT =
    "dragon.cache.local.keep.pct";
	public static final String SYSTEM_DIR = null;
	public static final String STAGING_AREA_ROOT = null;
  
  public static final String RESOURCE_CALCULATOR_PLUGIN = 
      "dragon.job.resourcecalculatorplugin";
}