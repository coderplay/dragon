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
package org.apache.hadoop.realtime.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * An LRU map, based on <code>LinkedHashMap</code>.
 *
 * <p>
 * This map has a fixed maximum number of elements (<code>maxSize</code>).
 * If the map is full and another entry is added,
 * the LRU (least recently used) entry is dropped.
 *
 * <p>
 * This class is thread-safe. All methods of this class are synchronized.
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {

  private static final int DEFAULT_MAX_SIZE = 100;
  private static final float hashTableLoadFactor = 0.75f;

  private static final Log LOG = LogFactory.getLog(LRUMap.class);

  // Kryo is not thread safe. Each thread should have its own Kryo instance.
  private final Kryo kryo;

  // maximum number of elements in map
  private int maxSize;


  public LRUMap(final int maxSize) {
    super(
        (int) Math.ceil(maxSize / hashTableLoadFactor) + 1,
        hashTableLoadFactor,
        true);

    this.maxSize = maxSize;
    this.kryo = createKryo();
  }

  @Override
  protected boolean removeEldestEntry(final Map.Entry<K,V> eldest) {
      return size() > maxSize;
  }

  /**
   * Retrieves an entry from the map.<br>
   * The retrieved entry becomes the MRU (most recently used) entry.
   * @param key the key whose associated value is to be returned.
   * @return    the value associated to this key, or null if no value with this key exists in the map.
   */
  @Override
  public synchronized V get (final Object key) {
    return super.get(key);
  }

  /**
   * Adds an entry to this map.
   * The new entry becomes the MRU (most recently used) entry.
   * If an entry with the specified key already exists in the map, it is replaced by the new entry.
   * If the map is full, the LRU (least recently used) entry is removed from the map.
   * @param key    the key with which the specified value is to be associated.
   * @param value  a value to be associated with the specified key.
   */
  @Override
  public synchronized V put (final K key, final V value) {
    return super.put(key, value);
  }

  /**
   * Clears the map.
   */
  public synchronized void clear() {
    super.clear();
  }

  /**
   * Returns the number of used entries in the map.
   * @return the number of entries currently in the map.
   */
  public synchronized int usedEntries() {
    return super.size();
  }

  /**
   * Returns a <code>Collection</code> that contains a copy of all map entries.
   * @return a <code>Collection</code> with a copy of the map content.
   */
  public synchronized Collection<Map.Entry<K,V>> getAll() {
    return new ArrayList<Map.Entry<K,V>>(super.entrySet());
  }

  /**
   * this method write entry bytes from least to most used entry
   *
   * LinkedHashMap link entries is in order
   * header <--> |least| <--> ... <--> |most| <--> header
   *
   * @return the bytes contain max size and entries in this map
   */
  public synchronized byte[] toByteArray() {
    ByteArrayOutputStream outputStream = null;
    Output output = null;
    try {
      outputStream = new ByteArrayOutputStream();
      output = new Output(outputStream);

      // write max size
      kryo.writeClassAndObject(output, maxSize);
      for (final Map.Entry<K,V> entry : super.entrySet()) {
        kryo.writeClassAndObject(output, entry);
      }

      output.flush();
      return outputStream.toByteArray();
    } finally {
      IOUtils.cleanup(LOG, output, outputStream);
    }
  }

  /**
   * this method read entries from bytes, and rebuild the map
   *
   * Entries in bytes is in order |least| <--> ... <--> |most|
   * last entry put into LinkedHashMap is most used one
   *
   * @param bytes  the bytes contain max size and entries
   */
  public static <K,V> LRUMap<K,V> fromByteArray(final byte[] bytes) {
    final Kryo kryo = createKryo();

    Input input = null;
    try {
      input = new Input(bytes);

      // read max size first
      final int maxSize = (Integer) kryo.readClassAndObject(input);

      final LRUMap<K,V> map = new LRUMap<K,V>(maxSize);
      for (int i = 0; i < maxSize; i++) {
        Map.Entry<K,V> entry = (Map.Entry<K, V>) kryo.readClassAndObject(input);
        map.put(entry.getKey(), entry.getValue());
      }

      return map;
    } finally {
      IOUtils.cleanup(LOG, input);
    }
  }

  private static Kryo createKryo() {
    final Kryo kryo = new Kryo();
    // we need support arguments constructor
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

    return kryo;
  }
}
