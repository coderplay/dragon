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
package org.apache.hadoop.realtime.io.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.realtime.DragonJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Base class for providing a fast serialization.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KryoSerialization<T> extends Configured implements
    Serialization<T> {
  
  private static final Log LOG =
      LogFactory.getLog(KryoSerialization.class.getName());

  class KryoSerializationSerializer<OUT> implements
      Serializer<OUT> {

    private final Class<OUT> type;
    private final Kryo kryo;
    private Output output;

    KryoSerializationSerializer(Class<OUT> type) {
      this.type = type;
      this.kryo = new Kryo();
      this.kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
      this.kryo.register(type);
 
      final String[] registerClasses = getConf().getStrings(
              DragonJobConfig.KRYO_SERIALIZATION_REGISTER_CLASSES);
      if (registerClasses != null) {
        try {
          for (final String clazz : registerClasses) {
            this.kryo.register(getConf().getClassByName(clazz));
          }
        } catch (ClassNotFoundException e) {
          LOG.warn("Kryo registration class not found: "
              + StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void close() throws IOException {
      output.close();
    }

    @Override
    public void open(OutputStream outStream) throws IOException {
      this.output = new Output(outStream);
    }

    @Override
    public void serialize(OUT object) throws IOException {
      kryo.writeObject(output, object);
    }

  }

  class KryoSerializationDeserializer<IN> implements
      Deserializer<IN> {

    private final Class<IN> type;
    private final Kryo kryo;
    private Input input;

    KryoSerializationDeserializer(Class<IN> type) {
      this.type = type;
      this.kryo = new Kryo();
      this.kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
      this.kryo.register(type);

      final String[] registerClasses = getConf().getStrings(
              DragonJobConfig.KRYO_SERIALIZATION_REGISTER_CLASSES);
      if (registerClasses != null) {
        try {
          for (final String clazz : registerClasses) {
            this.kryo.register(getConf().getClassByName(clazz));
          }
        } catch (ClassNotFoundException e) {
          LOG.warn("Kryo registration class not found: "
              + StringUtils.stringifyException(e));
        }
      }
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public IN deserialize(IN object) throws IOException {
      // ignore passed-in object
      return kryo.readObject(input, type);
    }

    @Override
    public void open(InputStream inStream) throws IOException {
      this.input = new Input(inStream);
    }

  }

  @Override
  public boolean accept(Class<?> c) {
    return true;
  }

  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return new KryoSerializationSerializer<T>(c);
  }

  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return new KryoSerializationDeserializer<T>(c);
  }

}
