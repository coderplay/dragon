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
package org.apache.hadoop.realtime.examples;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJob;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.realtime.DragonJobGraph;

public class DragonExample {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    setJarByClass(conf,DragonJob.class);
    DragonJob job = DragonJob.getInstance(conf);
    job.setJobName("First Graph Job");  
    DragonJobGraph g = new DragonJobGraph();
    job.setJobGraph(g);
    // check all source vertex hold event producers when submitting
    job.submit();
    System.exit(job.monitorAndPrintJob() ? 0 : 1);
  }

  public static void setJarByClass(Configuration conf,Class cls) {  
    String jar = findContainingJar(cls);
    if (jar != null) {  
      conf.set(DragonJobConfig.JAR, jar);
    }
    
  }  
  /** 
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   * 
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  public static String findContainingJar(Class my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for(Enumeration itr = loader.getResources(class_file);
          itr.hasMoreElements();) {
        URL url = (URL) itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
