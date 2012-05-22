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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.ContainerLogAppender;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;

/**
 * Helper class for Dragon applications
 */
@Private
@Unstable
public class DragonApps extends Apps {
  public static final String JOB = "job";
  public static final String TASK = "task";
  public static final String ATTEMPT = "attempt";

  public static void setupDistributedCache( 
      Configuration conf, 
      Map<String, LocalResource> localResources) 
  throws IOException {
    
    // Cache archives
    parseDistributedCache(conf, localResources,  
        LocalResourceType.ARCHIVE, 
        getCacheArchives(conf), 
        getArchiveTimestamps(conf), 
        getFileSizes(conf, DragonJobConfig.CACHE_ARCHIVES_SIZES), 
        getArchiveVisibilities(conf), 
        getArchiveClassPaths(conf));
    
    // Cache files
    parseDistributedCache(conf, 
        localResources,  
        LocalResourceType.FILE, 
        getCacheFiles(conf),
        getFileTimestamps(conf),
        getFileSizes(conf, DragonJobConfig.CACHE_FILES_SIZES),
        getFileVisibilities(conf),
        getFileClassPaths(conf));
  }
  
  private static void parseDistributedCache(
      Configuration conf,
      Map<String, LocalResource> localResources,
      LocalResourceType type,
      URI[] uris, long[] timestamps, long[] sizes, boolean visibilities[], 
      Path[] pathsToPutOnClasspath) throws IOException {

    if (uris != null) {
      // Sanity check
      if ((uris.length != timestamps.length) || (uris.length != sizes.length) ||
          (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for " +
            "distributed-cache artifacts of type " + type + " :" +
            " #uris=" + uris.length +
            " #timestamps=" + timestamps.length +
            " #visibilities=" + visibilities.length
            );
      }
      
      Map<String, Path> classPaths = new HashMap<String, Path>();
      if (pathsToPutOnClasspath != null) {
        for (Path p : pathsToPutOnClasspath) {
          FileSystem remoteFS = p.getFileSystem(conf);
          p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
              remoteFS.getWorkingDirectory()));
          classPaths.put(p.toUri().getPath().toString(), p);
        }
      }
      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u);
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));
        // Add URI fragment or just the filename
        Path name = new Path((null == u.getFragment())
          ? p.getName()
          : u.getFragment());
        if (name.isAbsolute()) {
          throw new IllegalArgumentException("Resource name must be relative");
        }
        String linkName = name.toUri().getPath();
        localResources.put(
            linkName,
            BuilderUtils.newLocalResource(
                p.toUri(), type, 
                visibilities[i]
                  ? LocalResourceVisibility.PUBLIC
                  : LocalResourceVisibility.PRIVATE,
                sizes[i], timestamps[i])
        );
      }
    }
  }
  
  private static void setDragonFrameworkClasspath(
      Map<String, String> environment, Configuration conf) throws IOException {
    InputStream classpathFileStream = null;
    BufferedReader reader = null;
    try {
      // Get yarn mapreduce-app classpath from generated classpath
      // Works if compile time env is same as runtime. Mainly tests.
      ClassLoader thisClassLoader =
          Thread.currentThread().getContextClassLoader();
      String mrAppGeneratedClasspathFile = "mrapp-generated-classpath";
      classpathFileStream =
          thisClassLoader.getResourceAsStream(mrAppGeneratedClasspathFile);

      // Put the file itself on classpath for tasks.
      URL classpathResource = thisClassLoader
        .getResource(mrAppGeneratedClasspathFile);
      if (classpathResource != null) {
        String classpathElement = classpathResource.getFile();
        if (classpathElement.contains("!")) {
          classpathElement = classpathElement.substring(0,
            classpathElement.indexOf("!"));
        } else {
          classpathElement = new File(classpathElement).getParent();
        }
        Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          classpathElement);
      }

      if (classpathFileStream != null) {
        reader = new BufferedReader(new InputStreamReader(classpathFileStream));
        String cp = reader.readLine();
        if (cp != null) {
          Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
            cp.trim());
        }
      }

      // Add standard Hadoop classes
      for (String c : conf.get(YarnConfiguration.YARN_APPLICATION_CLASSPATH)
          .split(",")) {
        Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), c
            .trim());
      }
    } finally {
      if (classpathFileStream != null) {
        classpathFileStream.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
    // TODO: Remove duplicates.
  }
  
  public static void setClasspath(Map<String, String> environment,
      Configuration conf) throws IOException {
    boolean userClassesTakesPrecedence = 
      conf.getBoolean(DragonJobConfig.DRAGON_JOB_USER_CLASSPATH_FIRST, false);

    if (!userClassesTakesPrecedence) {
      DragonApps.setDragonFrameworkClasspath(environment, conf);
    }
    Apps.addToEnvironment(
        environment,
        Environment.CLASSPATH.name(),
        DragonJobConfig.JOB_JAR);
    Apps.addToEnvironment(
        environment,
        Environment.CLASSPATH.name(),
        Environment.PWD.$() + Path.SEPARATOR + "*");
    if (userClassesTakesPrecedence) {
      DragonApps.setDragonFrameworkClasspath(environment, conf);
    }
  }
  
  private static final String STAGING_CONSTANT = ".staging";
  public static Path getStagingAreaDir(Configuration conf, String user) {
    return new Path(
        conf.get(DragonJobConfig.DRAGON_AM_STAGING_DIR) + 
        Path.SEPARATOR + user + Path.SEPARATOR + STAGING_CONSTANT);
  }
  
  
  /**
   * Add the JVM system properties necessary to configure {@link ContainerLogAppender}.
   * @param logLevel the desired log level (eg INFO/WARN/DEBUG)
   * @param logSize See {@link ContainerLogAppender#setTotalLogFileSize(long)}
   * @param vargs the argument list to append to
   */
  public static void addLog4jSystemProperties(
      String logLevel, long logSize, List<String> vargs) {
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-D" + DragonJobConfig.TASK_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    vargs.add("-D" + DragonJobConfig.TASK_LOG_SIZE + "=" + logSize);
    vargs.add("-Dhadoop.root.logger=" + logLevel + ",CLA"); 
  }
  
  /**
   * Get cache archives set in the Configuration. 
   * @param conf The configuration which contains the archives
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */  
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(DragonJobConfig.CACHE_ARCHIVES));
  }
  /**
   * Get cache files set in the Configuration. 
   * @param conf The configuration which contains the files
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */
  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(DragonJobConfig.CACHE_FILES));
  }
  /**
   * Get the timestamps of the archives.
   * @param conf The configuration which stored the timestamps
   * @return a string array of timestamps 
   * @throws IOException
   */
  public static long[] getArchiveTimestamps(Configuration conf) {
    return parseTimeStamps(conf.getStrings(DragonJobConfig.CACHE_ARCHIVES_TIMESTAMPS));
  }


  /**
   * Get the timestamps of the files.  
   * @param conf The configuration which stored the timestamps
   * @return a string array of timestamps 
   * @throws IOException
   */
  public static long[] getFileTimestamps(Configuration conf) {
    return parseTimeStamps(conf.getStrings(DragonJobConfig.CACHE_FILE_TIMESTAMPS));
  }
  
  /**
   * Get the file entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   * 
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getFileClassPaths(Configuration conf) {
    ArrayList<String> list = (ArrayList<String>)conf.getStringCollection(
                                DragonJobConfig.CLASSPATH_FILES);
    if (list.size() == 0) { 
      return null; 
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }
  /**
   * Get the archive entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   * 
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getArchiveClassPaths(Configuration conf) {
    ArrayList<String> list = (ArrayList<String>)conf.getStringCollection(
                                DragonJobConfig.CLASSPATH_ARCHIVES);
    if (list.size() == 0) { 
      return null; 
    }
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path(list.get(i));
    }
    return paths;
  }

  private static long[] getFileSizes(Configuration conf, String key) {
    String[] strs = conf.getStrings(key);
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }
  /**
   * Get the booleans on whether the files are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   * @throws IOException
   */
  public static boolean[] getFileVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(DragonJobConfig.CACHE_FILE_VISIBILITIES));
  }
  
  /**
   * Get the booleans on whether the archives are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a string array of booleans 
   */
  public static boolean[] getArchiveVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(DragonJobConfig.CACHE_ARCHIVES_VISIBILITIES));
  }
  
  
  private static long[] parseTimeStamps(String[] strs) {
    if (null == strs) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }
  
  private static boolean[] parseBooleans(String[] strs) {
    if (null == strs) {
      return null;
    }
    boolean[] result = new boolean[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Boolean.parseBoolean(strs[i]);
    }
    return result;
  }
}
