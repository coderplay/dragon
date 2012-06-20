package org.apache.hadoop.realtime;
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
    job.setName("First Graph Job");
    DragonVertex source = new DragonVertex.Builder("source")
                                             .tasks(2)
                                             .build();
    DragonVertex m1 = new DragonVertex.Builder("intermediate1")
                                         .tasks(2)
                                         .build();
    DragonJobGraph g = new DragonJobGraph();
    g.addEdge(source, m1);
    job.setJobGraph(g);
    // check all source vertex hold event producers when submitting
    job.submit();
    System.exit(job.monitorAndPrintJob() ? 0 : 1);
  }
  public static void setJarByClass(Configuration conf,Class cls) {  
    String jar = findContainingJar(cls);
    if (jar != null) {  
      conf.set(DragonJobConfig.JOB_JAR, jar);
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

