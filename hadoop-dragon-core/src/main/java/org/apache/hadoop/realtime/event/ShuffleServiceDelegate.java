package org.apache.hadoop.realtime.event;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShuffleServiceDelegate {

  private static final Log LOG = LogFactory
      .getLog(ShuffleServiceDelegate.class);

  private List<InetSocketAddress> nmAddresses;

  public ShuffleServiceDelegate() {
    this.nmAddresses = getNodeManagerAddress();
  }

  public boolean emitEvent() throws InterruptedException {
    try {
      // TODO: call real proxy send message
    } catch (Exception e) {
      Thread.sleep(100);
      nmAddresses = getNodeManagerAddress();
      LOG.warn("try send message again.");
      // TODO: call real proxy send message
    }
    return false;
  }

  private List<InetSocketAddress> getNodeManagerAddress() {
    return null;
  }
}
