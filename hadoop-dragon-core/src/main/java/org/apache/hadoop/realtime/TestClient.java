package org.apache.hadoop.realtime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.DragonJob;
import org.apache.hadoop.realtime.DragonJobConfig;

public class TestClient {

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
    Configuration conf=new Configuration();
    conf.set(DragonJobConfig.JAR, args[0]);
    DragonJob job=DragonJob.getInstance(conf);
    job.submit();
    job.monitorAndPrintJob();
  }
}
