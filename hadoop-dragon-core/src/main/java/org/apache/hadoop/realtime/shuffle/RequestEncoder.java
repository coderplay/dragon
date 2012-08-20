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
package org.apache.hadoop.realtime.shuffle;

import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.util.PureJavaCrc32;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

/**
 * This command is used for sending events to node manager.
 * <p>
 * Wire format:<br>
 * <ul>
 * <li>version, 1 byte, command version</li>
 * <li>command, 1 byte</li>
 * <li>attributes, 1 byte</li>
 * <li>checksum,4 bytes</li>
 * <li>length (include ), n bytes, variable-length encode, includes the length of
 * metadata and payload</li>
 * <li>source task id, 2 bytes</li>
 * <li>metadata, 1 byte, metadata of payload </load>
 * <li>payload</li>
 * </ul>
 * 
 * <pre>
 * +------------+------------+------------+------------+ 0  bytes
 * |   version  |     put    | attributes |            |
 * +------------+------------+------------+------------+ 4  bytes
 * |               checksum               |            |
 * +--------------------------------------+------------+ 8  bytes
 * |        playload  length (vlen)       |            |
 * +--------------------------------------+------------+ 12 bytes
 * |                       payload                     |
 * +---------------------------------------------------+
 * |                        ...                        |
 * +---------------------------------------------------+ ...
 * </pre>
 * 
 * Payload is made up with one or more events.
 * <p>
 * Event format:
 * <ul>
 * <li>source event offset, 8 bytes, be used to eliminates duplicate events</li>
 * <li>event length, n bytes, variable-length encode</li>
 * <li>event data</i>
 * </ul>
 * 
 * <pre>
 * +---------------------------------------------------+ 0  bytes
 * |                  source event offset              |
 * +---------------------------------------------------+ 4  byte
 * |                                                   |
 * +---------------------------------------------------+ 8  bytes
 * |                    event length                   |
 * +---------------------------------------------------+
 * |                     event data                    |
 * |                        ...                        |
 * +---------------------------------------------------+ ...
 * </pre>
 */
class RequestEncoder extends OneToOneEncoder {

  private static final byte REQ = 0;

  
  final Serializer<Object> serializer;

  public RequestEncoder(Configuration conf) {
    SerializationFactory factory = new SerializationFactory(conf);
    serializer = factory.getSerializer(Object.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.jboss.netty.handler.codec.oneone.OneToOneEncoder#encode(org.jboss.netty
   * .channel.ChannelHandlerContext, org.jboss.netty.channel.Channel,
   * java.lang.Object)
   */
  @Override
  protected Object encode(ChannelHandlerContext ctx, 
                          Channel channel,
                          Object msg)
          throws Exception {
    if (!(msg instanceof Event)) {
      // Ignore what this encoder can't encode.
      return msg;
    }

    Event message = (Event) msg;
    ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
    buf.writeByte(1);                                     // version
    buf.writeByte(REQ);                                   // request
    buf.writeByte(0);                                     // attributes
    
    int crcIndex = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 4 + 4);

    buf.writeLong(message.offset());                      // event offset
                                                          // event length
    ChannelBufferOutputStream cbos = new ChannelBufferOutputStream(buf);
    CheckedOutputStream eventChk =
        new CheckedOutputStream(cbos, new PureJavaCrc32());
    serializer.open(eventChk);
    serializer.serialize(message.key());
    serializer.serialize(message.value());
    serializer.close();

    int end = buf.writerIndex();    
    int len =  end - (crcIndex + 4 + 4);
    buf.writerIndex(crcIndex);
    buf.writeInt((int)eventChk.getChecksum().getValue()); // checksum
    buf.writeInt(len);                                    // payload length
    buf.writerIndex(end);
    return buf;
  }

}
