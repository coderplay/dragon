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
package org.apache.hadoop.realtime.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.realtime.DragonJob;
import org.apache.hadoop.realtime.event.Event;
import org.apache.hadoop.realtime.mr.MapContext;
import org.apache.hadoop.realtime.mr.Mapper;
import org.apache.hadoop.realtime.mr.ReduceContext;
import org.apache.hadoop.realtime.mr.Reducer;
import org.apache.hadoop.realtime.util.LRUMap;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * word count example for dragon job
 */
public class WordCount {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    protected void map(
        Event<Object, Text> event,
        MapContext<Object, Text, Text, IntWritable> context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(event.value().toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.emitEvent(new Event<Text, IntWritable>(word, one));
      }

    }
  }

  public static class CountReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {

    private Map<Text, IntWritable> countMap =
        new LRUMap<Text, IntWritable>(1024 * 1024 * 20);

    @Override
    protected void reduce(Event<Text, IntWritable> event,
        ReduceContext<Text, IntWritable, Text, IntWritable> reduceContext)
        throws IOException, InterruptedException {
      Text word = event.key();
      IntWritable wordCount = countMap.get(word);
      if (wordCount == null) {
        countMap.put(word, event.value());
      } else {
        wordCount.set(wordCount.get() + event.value().get());
      }

      reduceContext.emitEvent(new Event<Text, IntWritable>(word, wordCount));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    DragonJob job = DragonJob.getInstance(configuration);

    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CountReducer.class);
    job.setName("word count");

    job.submit();
  }
}
