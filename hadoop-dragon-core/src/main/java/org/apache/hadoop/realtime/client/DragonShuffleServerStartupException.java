package org.apache.hadoop.realtime.client;


public class DragonShuffleServerStartupException extends RuntimeException {

  static final long serialVersionUID = -1L;


  public DragonShuffleServerStartupException() {
      super();

  }


  public DragonShuffleServerStartupException(String message, Throwable cause) {
      super(message, cause);

  }


  public DragonShuffleServerStartupException(String message) {
      super(message);

  }


  public DragonShuffleServerStartupException(Throwable cause) {
      super(cause);

  }
}
