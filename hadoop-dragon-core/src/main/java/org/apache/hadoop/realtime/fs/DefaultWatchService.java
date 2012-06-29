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
package org.apache.hadoop.realtime.fs;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Simple WatchService implementation that uses periodic tasks to poll
 * registered directories for changes. This implementation is for use on
 * operating systems that do not have native file change notification support.
 */

class DefaultWatchService extends AbstractWatchService implements Configurable {
  // map of registrations
  private final Map<Path, DefaultWatchKey> map =
      new HashMap<Path, DefaultWatchKey>();

  // used to execute the periodic tasks that poll for changes
  private final ScheduledExecutorService scheduledExecutor;

  private Configuration config;

  DefaultWatchService() {
    scheduledExecutor =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
          }
        });
  }

  /**
   * Register the given file with this watch service
   */
  @Override
  WatchKey register(final Path path, WatchEvent.Kind<?>... events)
      throws IOException {
    // check events - CCE will be thrown if there are invalid elements
    if (events.length == 0)
      throw new IllegalArgumentException("No events to register");
    final Set<WatchEvent.Kind<?>> eventSet =
        new HashSet<WatchEvent.Kind<?>>(events.length);
    for (WatchEvent.Kind<?> event : events) {
      // standard events
      if (event == StandardWatchEventKinds.ENTRY_CREATE
          || event == StandardWatchEventKinds.ENTRY_MODIFY
          || event == StandardWatchEventKinds.ENTRY_DELETE) {
        eventSet.add(event);
        continue;
      }

      // OVERFLOW is ignored
      if (event == StandardWatchEventKinds.OVERFLOW) {
        if (events.length == 1)
          throw new IllegalArgumentException("No events to register");
        continue;
      }

      // null/unsupported
      if (event == null)
        throw new NullPointerException("An element in event set is 'null'");
      throw new UnsupportedOperationException(event.name());
    }

    // check if watch service is closed
    if (!isOpen())
      throw new ClosedWatchServiceException();

    // registration is done in privileged block as it requires the
    // attributes of the entries in the directory.
    try {
      return AccessController
          .doPrivileged(new PrivilegedExceptionAction<DefaultWatchKey>() {
            @Override
            public DefaultWatchKey run() throws IOException {
              return doPrivilegedRegister(path, eventSet);
            }
          });
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      if (cause != null && cause instanceof IOException)
        throw (IOException) cause;
      throw new AssertionError(pae);
    }
  }

  // registers directory returning a new key if not already registered or
  // existing key if already registered
  private DefaultWatchKey doPrivilegedRegister(Path path,
      Set<? extends WatchEvent.Kind<?>> events) throws IOException {
    // check file is a directory and get its file key if possible
    FileSystem fs = path.getFileSystem(getConf());
    if (!fs.getFileStatus(path).isDir()) {
      throw new IOException(path.getName() + " is not a directory.");
    }
    // grab close lock to ensure that watch service cannot be closed
    synchronized (closeLock()) {
      if (!isOpen())
        throw new ClosedWatchServiceException();

      DefaultWatchKey watchKey;
      synchronized (map) {
        watchKey = map.get(path);
        if (watchKey == null) {
          // new registration
          watchKey = new DefaultWatchKey(path, this);
          map.put(path, watchKey);
        } else {
          // update to existing registration
          watchKey.disable();
        }
      }
      watchKey.enable(events,
          getConf().getLong("hdfs.watchservice.interval", 1000));
      return watchKey;
    }

  }

  @Override
  void implClose() throws IOException {
    synchronized (map) {
      for (Map.Entry<Path, DefaultWatchKey> entry : map.entrySet()) {
        DefaultWatchKey watchKey = entry.getValue();
        watchKey.disable();
        watchKey.invalidate();
      }
      map.clear();
    }
    AccessController.doPrivileged(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        scheduledExecutor.shutdown();
        return null;
      }
    });
  }

  /**
   * Entry in directory cache to record file last-modified-time and tick-count
   */
  private static class CacheEntry {
    private long lastModified;
    private int lastTickCount;

    CacheEntry(long lastModified, int lastTickCount) {
      this.lastModified = lastModified;
      this.lastTickCount = lastTickCount;
    }

    int lastTickCount() {
      return lastTickCount;
    }

    long lastModified() {
      return lastModified;
    }

    void update(long lastModified, int tickCount) {
      this.lastModified = lastModified;
      this.lastTickCount = tickCount;
    }
  }

  /**
   * WatchKey implementation that encapsulates a map of the entries of the
   * entries in the directory. Polling the key causes it to re-scan the
   * directory and queue keys when entries are added, modified, or deleted.
   */
  private class DefaultWatchKey extends AbstractWatchKey {
    private final Path fileKey;

    // current event set
    private Set<? extends WatchEvent.Kind<?>> events;

    // the result of the periodic task that causes this key to be polled
    private ScheduledFuture<?> poller;

    // indicates if the key is valid
    private volatile boolean valid;

    // used to detect files that have been deleted
    private int tickCount;

    // map of entries in directory
    private Map<Path, CacheEntry> entries;

    DefaultWatchKey(Path dir, DefaultWatchService watcher) throws IOException {
      super(dir, watcher);
      this.fileKey = dir;
      this.valid = true;
      this.tickCount = 0;
      this.entries = new HashMap<Path, CacheEntry>();

      // get the initial entries in the directory
      FileSystem fs = watchable().getFileSystem(getConf());
      FileStatus[] files = fs.globStatus(watchable());
      for (FileStatus entry : files) {
        long lastModified = entry.getModificationTime();
        entries.put(entry.getPath(), new CacheEntry(lastModified, tickCount));
      }
    }

    Path fileKey() {
      return fileKey;
    }

    @Override
    public boolean isValid() {
      return valid;
    }

    void invalidate() {
      valid = false;
    }

    // enables periodic polling
    void enable(Set<? extends WatchEvent.Kind<?>> events, long period) {
      synchronized (this) {
        // update the events
        this.events = events;

        // create the periodic task
        Runnable thunk = new Runnable() {
          public void run() {
            poll();
          }
        };
        this.poller =
            scheduledExecutor.scheduleAtFixedRate(thunk, period, period,
                TimeUnit.MILLISECONDS);
      }
    }

    // disables periodic polling
    void disable() {
      synchronized (this) {
        if (poller != null)
          poller.cancel(false);
      }
    }

    @Override
    public void cancel() {
      valid = false;
      synchronized (map) {
        map.remove(fileKey());
      }
      disable();
    }

    /**
     * Polls the directory to detect for new files, modified files, or deleted
     * files.
     */
    synchronized void poll() {
      if (!valid) {
        return;
      }

      // update tick
      tickCount++;

      // open directory
      FileStatus[] files = null;
      try {
        FileSystem fs = watchable().getFileSystem(getConf());
        files = fs.listStatus(watchable());
      } catch (IOException x) {
        // directory is no longer accessible so cancel key
        cancel();
        signal();
        return;
      }

      // iterate over all entries in directory
      for (FileStatus entry : files) {
        long lastModified = entry.getModificationTime();
        // lookup cache
        CacheEntry e = entries.get(entry.getPath());
        if (e == null) {
          // new file found
          entries.put(entry.getPath(), new CacheEntry(lastModified, tickCount));

          // queue ENTRY_CREATE if event enabled
          if (events.contains(StandardWatchEventKinds.ENTRY_CREATE)) {
            signalEvent(StandardWatchEventKinds.ENTRY_CREATE, entry.getPath());
            continue;
          } else {
            // if ENTRY_CREATE is not enabled and ENTRY_MODIFY is
            // enabled then queue event to avoid missing out on
            // modifications to the file immediately after it is
            // created.
            if (events.contains(StandardWatchEventKinds.ENTRY_MODIFY)) {
              signalEvent(StandardWatchEventKinds.ENTRY_MODIFY, entry.getPath());
            }
          }
          continue;
        }

        // check if file has changed
        if (e.lastModified != lastModified) {
          if (events.contains(StandardWatchEventKinds.ENTRY_MODIFY)) {
            signalEvent(StandardWatchEventKinds.ENTRY_MODIFY, entry.getPath());
          }
        }
        // entry in cache so update poll time
        e.update(lastModified, tickCount);

      }

      // iterate over cache to detect entries that have been deleted
      Iterator<Map.Entry<Path, CacheEntry>> i = entries.entrySet().iterator();
      while (i.hasNext()) {
        Map.Entry<Path, CacheEntry> mapEntry = i.next();
        CacheEntry entry = mapEntry.getValue();
        if (entry.lastTickCount() != tickCount) {
          Path name = mapEntry.getKey();
          // remove from map and queue delete event (if enabled)
          i.remove();
          if (events.contains(StandardWatchEventKinds.ENTRY_DELETE)) {
            signalEvent(StandardWatchEventKinds.ENTRY_DELETE, name);
          }
        }
      }
    }
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }
}
