package org.apache.hadoop.realtime.child;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.realtime.records.TaskAttemptId;
import org.apache.hadoop.realtime.records.TaskAttemptReport;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public class MockChildServiceDelegate extends ChildServiceDelegate {

  private static final Log LOG = LogFactory.getLog(MockChildServiceDelegate.class);
  
  public MockChildServiceDelegate(Configuration conf, String jobId,
      InetSocketAddress amAddress) {
    super(conf, jobId, amAddress);
    // TODO Auto-generated constructor stub
  }

  public boolean ping(TaskAttemptId jobId) throws YarnRemoteException {
    LOG.info("Ping called.");
    return true;
  }
  public boolean statusUpdate(TaskAttemptId attemptId, TaskAttemptReport taskReport)
      throws YarnRemoteException {
    LOG.info("StatusUpdate called.");
    LOG.info(taskReport.getCounters());
    return true;
  }
}
