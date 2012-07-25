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
package org.apache.hadoop.realtime.event;

import java.io.Closeable;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.realtime.fs.StandardWatchEventKinds;
import org.apache.hadoop.realtime.fs.WatchEvent;
import org.apache.hadoop.realtime.fs.WatchKey;
import org.apache.hadoop.realtime.fs.WatchService;
import org.apache.hadoop.realtime.fs.WatchServiceFactory;
import org.apache.hadoop.realtime.records.ChildExecutionContext;

/**
 */
public class DefaultEventProducer<KEYIN, VALUEIN> implements
    EventProducer<KEYIN, VALUEIN>, Closeable {
  private static final Log LOG = LogFactory.getLog(DefaultEventProducer.class);

  private static final NumberFormat fileNameFormat;
  static {
    fileNameFormat = NumberFormat.getInstance();
    fileNameFormat.setMinimumIntegerDigits(20);
    fileNameFormat.setMaximumFractionDigits(0);
    fileNameFormat.setGroupingUsed(false);
  }
  private static final String FILE_SUFFIX = ".drg";
  /* 20 digitals + FILE_SUFFIX */
  private static final Pattern FILE_PATTERN = Pattern
      .compile("\\d{20}\\" + FILE_SUFFIX);

  private volatile boolean running = true;

  private final Configuration conf;
  private final FileSystem fileSystem;
  private final Path parentDir;
  
  private FSDataInputStream in;

  private WatchService watchService;

  private final BlockingDeque<Path> fifoQueue =
      new LinkedBlockingDeque<Path>();

  private final ExecutorService singleExecutor = Executors
      .newSingleThreadExecutor();

  public DefaultEventProducer(final Configuration conf, final Path path,
      final int bufferSize) throws IOException {
    this.conf = conf;
    this.fileSystem = FileSystem.get(conf);
    this.parentDir = path;
    startWatchService();
  }
  

  public DefaultEventProducer(final Configuration conf, final Path path,
      final int bufferSize, final long startOffset) throws IOException {
    this.conf = conf;
    this.fileSystem = FileSystem.get(conf);
    this.parentDir = path;
    initByOffset(fileSystem, parentDir, startOffset);
    startWatchService();
  }

  @Override
  public void initialize(ChildExecutionContext context) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub
    
  }

  /**
   * take a event from the queue.
   * 
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public Event<KEYIN, VALUEIN> pollEvent() throws IOException,
      InterruptedException {
    FSDataInputStream input = getInputStream();
    if (input == null) {
      return null;
    }

    return null;
  }

  /**
   * take a event from the queue.
   * 
   * @return the current key object or null if there isn't one
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public Event<KEYIN, VALUEIN> pollEvent(long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    return null;
  }

  private void startWatchService() throws IOException {
    this.watchService = WatchServiceFactory.newWatchService(this.conf);
    this.watchService.register(this.parentDir,
        StandardWatchEventKinds.ENTRY_CREATE);
    this.singleExecutor.execute(new Runnable() {
      public void run() {
        // forever polling
        while (DefaultEventProducer.this.running) {
          WatchKey key;
          try {
            key = DefaultEventProducer.this.watchService.take();
          } catch (InterruptedException x) {
            continue;
          }
          // break if the directory was removed
          if (!DefaultEventProducer.this.processEvents(key)) {
            break;
          }
        }

        try {
          DefaultEventProducer.this.watchService.close();
        } catch (IOException ioe) {
          // log here
        }
      }
    });
  }

  static String nameFromOffset(final long offset) {
    return fileNameFormat.format(offset) + FILE_SUFFIX;
  }

  static long startOffsetFromName(final String name) {
    return Long.parseLong(name.substring(0,
        name.length() - FILE_SUFFIX.length()));
  }

  private void initByOffset(final FileSystem fs, final Path parent, final long offset)
      throws IOException {
    FileStatus[] statuses = fs.listStatus(parent, new PathFilter() {
      public boolean accept(Path path) {
        return FILE_PATTERN.matcher(path.getName()).matches()
            && (nameFromOffset(offset).compareTo(path.getName()) >= 0);
      }
    });

    if (statuses.length == 0) {
      LOG.info("There is no existing file with its start offset larger than "
          + offset);
      return;
    }

    Arrays.sort(statuses, new Comparator<FileStatus>() {
      @Override
      public int compare(final FileStatus lhs, final FileStatus rhs) {
        return lhs.getPath().compareTo(rhs.getPath());
      }
    });

    // FIXME: must consider about the length of file header
    FileStatus lastFile = statuses[statuses.length - 1];
    long lastFileOffset = startOffsetFromName(lastFile.getPath().getName());
    if (offset > (lastFileOffset + lastFile.getLen())) {
      LOG.info("The file corresponding to the offset " + offset
          + " has been deleted.");
      return;
    }

    for (FileStatus status : statuses) {
      // add file into the queue only if it's newer than the tail of the queue
      this.fifoQueue.offer(status.getPath());
    }
  }
  
  private synchronized FSDataInputStream getInputStream() throws IOException {
    if (this.in == null) {
      Path path = this.fifoQueue.peek();
      if (path == null) {
        return null;
      }
      this.in = this.fileSystem.open(path);
      LOG.info("Opening file" + path.toUri().getPath());
    }
    return this.in;
  }

  @SuppressWarnings({ "unchecked" })
  private synchronized boolean processEvents(WatchKey key) {
    List<Path> newList = new ArrayList<Path>();

    for (WatchEvent<?> event : key.pollEvents()) {
      WatchEvent<Path> ev = (WatchEvent<Path>) event;
      Path eventPath = ev.context();
      String realPath = eventPath.getName();
      if (ev.kind() == StandardWatchEventKinds.ENTRY_CREATE
          || ev.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
        if (DefaultEventProducer.FILE_PATTERN.matcher(realPath).matches()) {
          newList.add(eventPath);
        }
      }
    }
    Collections.sort(newList);
    Path tail = this.fifoQueue.peekLast();
    // add to the queue
    for (Path path : newList) {
      // add file into the queue only if it's newer than the tail of the queue
      if (path.getName().endsWith(FILE_SUFFIX)
          && (tail == null || path.compareTo(tail) > 0)) {
        this.fifoQueue.offer(path);
      } else {
        LOG.info("ignore path " + path.toUri().getPath());
      }
    }
    return key.reset();
  }

  @Override
  public void close() throws IOException {
    this.running = false;
    this.singleExecutor.shutdown();
    this.watchService.close();
  }

}
