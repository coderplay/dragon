package org.apache.hadoop.realtime.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.realtime.records.JobId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.service.AbstractService;

import com.taobao.gecko.service.RemotingFactory;
import com.taobao.gecko.service.RemotingServer;
import com.taobao.gecko.service.config.ServerConfig;
import com.taobao.gecko.service.exception.NotifyRemotingException;

public class ShuffleHandler extends AbstractService implements
    AuxServices.AuxiliaryService {

  private static final Log LOG = LogFactory.getLog(ShuffleHandler.class);

  private int port;

  public static final String DRAGON_SHUFFLE_SERVICEID = "dragon.shuffle";
  public static final String SHUFFLE_PORT_CONFIG_KEY = "dragon.shuffle.port";
  public static final int DEFAULT_SHUFFLE_PORT = 8181;

  private static final Map<String, String> userRsrc =
      new ConcurrentHashMap<String, String>();

  /* TODO: Init the metrics manager
   * @Metrics(about = "Shuffle output metrics", context = "mapred") static class
   * ShuffleMetrics implements ChannelFutureListener {
   * 
   * @Metric("Shuffle output in bytes") MutableCounterLong shuffleOutputBytes;
   * 
   * @Metric("# of failed shuffle outputs") MutableCounterInt
   * shuffleOutputsFailed;
   * 
   * @Metric("# of succeeeded shuffle outputs") MutableCounterInt
   * shuffleOutputsOK;
   * 
   * @Metric("# of current shuffle connections") MutableGaugeInt
   * shuffleConnections;
   * 
   * @Override public void operationComplete(ChannelFuture future) throws
   * Exception { if (future.isSuccess()) { shuffleOutputsOK.incr(); } else {
   * shuffleOutputsFailed.incr(); } shuffleConnections.decr(); } }
   */

  private RemotingServer remotingServer;
  // private NMZookeeper zooKeeper;
  // final ShuffleMetrics metrics;

  ShuffleHandler(MetricsSystem ms) {
    super("dragon-shuffle");
    // metrics = ms.register(new ShuffleMetrics());

  }

  public ShuffleHandler() {
    this(DefaultMetricsSystem.instance());
  }

  private RemotingServer newRemotingServer(int port) {
    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.setWireFormatType(new DragonShuffleWireFormatType());
    // TODO: How to deal with the situation if the port has already used.
    serverConfig.setPort(port);
    final RemotingServer server =
        RemotingFactory.newRemotingServer(serverConfig);
    return server;
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * 
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    // TODO these bytes should be versioned
    DataOutputBuffer port_dob = new DataOutputBuffer();
    port_dob.writeInt(port);
    return ByteBuffer.wrap(port_dob.getData(), 0, port_dob.getLength());
  }

  /**
   * A helper function to deserialize the metadata returned by ShuffleHandler.
   * 
   * @param meta the metadata returned by the ShuffleHandler
   * @return the port the Shuffle Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    // TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    int port = in.readInt();
    return port;
  }

  @Override
  public ByteBuffer getMeta() {
    try {
      return serializeMetaData(port);
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  @Override
  public void initApp(String user, ApplicationId appId, ByteBuffer secret) {
    // TODO these bytes should be versioned
    try {
      // TODO: Serialize the bytes passed by DragonAppMaster
      // Token<JobTokenIdentifier> jt = deserializeServiceData(secret);
      JobId jobId = JobId.newJobId(appId, appId.getId());
      userRsrc.put(jobId.toString(), user);
    } catch (Exception e) {
      LOG.error("Error during initApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  public void stopApp(ApplicationId appId) {
    JobId jobId = JobId.newJobId(appId, appId.getId());
    // TODO: remove the information about this appId
    userRsrc.remove(jobId.toString());
  }

  @Override
  public synchronized void init(Configuration conf) {
    port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    remotingServer = newRemotingServer(port);
    // this.zooKeeper = new NMZooKeeper(conf);
    super.init(new Configuration(conf));
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  public synchronized void start() {
    try {
      this.remotingServer.start();
    } catch (final NotifyRemotingException e) {
      throw new DragonShuffleServerStartupException(
          "start remoting server failed", e);
    }
    LOG.info(getName() + " listening on port " + port);
    //this.zooKeeper.registerNMInZk();
    this.registerProcessors();
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping dragon shuffle server...");
    try {
      this.remotingServer.stop();
    } catch (final NotifyRemotingException e) {
      LOG.error("Shutdown remote server failed", e);
    }
    super.stop();
    LOG.info("Stop dragon shuffle server successfully");
  }

  private void registerProcessors() {
    // TODO: register processor of gecko
    // this.remotingServer.registerProcessor(GetCommand.class, new
    // GetProcessor(this.brokerProcessor,
    // this.executorsManager.getGetExecutor()));
  }
}
