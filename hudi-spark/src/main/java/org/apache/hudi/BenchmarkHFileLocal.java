/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BenchmarkHFileLocal {

  List<Path> seqPaths;
  List<Path> seekPaths;
  private static final String SEQUENTIAL = "sequential";
  private static final String SEEK = "seek";
  int buckets;
  int totalRecords;
  int totalRecordsPerBucket;
  List<List<String>> keySets = new ArrayList<>();
  Path basePath;
  Configuration conf;

  public static void main(String[] args) throws IOException {
    BenchmarkHFileLocal benchmarkHFileLocal = new BenchmarkHFileLocal();
    benchmarkHFileLocal.generateKeys();
    benchmarkHFileLocal.writeToFiles();
    benchmarkHFileLocal.readBenchmark();
  }

  public BenchmarkHFileLocal() {
    this(100, 10000000, 50000);
  }

  public BenchmarkHFileLocal(int buckets, int totalRecords, int totalRecordsToReadPerBucket) {
    this.buckets = buckets;
    this.totalRecords = totalRecords;
    this.totalRecordsPerBucket = totalRecordsToReadPerBucket;
    basePath = FileSystemTestUtils.getRandomOuterFSPath();
    conf = new Configuration();
    seqPaths = new ArrayList<>();
    seekPaths = new ArrayList<>();
  }

  void writeToFiles() throws IOException {

    // sequential
    for (int i = 0; i < buckets; i++) {
      Path seqPath = new Path(basePath.toString() + "/" + SEQUENTIAL + Integer.toString(i));
      seqPaths.add(seqPath);
      HFileUtils.writeToHFile(conf, seqPath, keySets.get(i), 1024);
      /* Path seekPath = new Path(basePath.toString() + "/" + SEEK + Integer.toString(i));
      seekPaths.add(seekPath);
      HFileUtils.writeToHFile(conf, seekPath, keySets.get(i));*/
    }
  }

  void readBenchmark() throws IOException {

    // sequential
    long totalTime1 = 0;
    for (int i = 0; i < buckets; i++) {
      Path bucketPath = seqPaths.get(i);
      List<String> keys = keySets.get(i);
      List<String> keystoSearch = keys.subList(0, totalRecordsPerBucket);
      Collections.sort(keystoSearch);
      long startTime = System.currentTimeMillis();
      HFileUtils.readAllRecordsSequentially(bucketPath, conf, HFileUtils.getKeys(keystoSearch));
      totalTime1 += (System.currentTimeMillis() - startTime);
    }
    long totalTime2 = 0;

    /* for (int i = 0; i < buckets; i++) {
      Path bucketPath = seekPaths.get(i);
      List<String> keys = keySets.get(i);
      List<String> keystoSearch = keys.subList(0, totalRecordsPerBucket);
      Collections.sort(keystoSearch);
      long startTime = System.currentTimeMillis();
      HFileUtils.readAllRecordsWithSeek(bucketPath, conf, HFileUtils.getKeys(keystoSearch));
      totalTime2 += (System.currentTimeMillis() - startTime);
    }*/
    System.out.println("SEQUENTIAL TOTAL TIME : " + totalTime1);
    System.out.println("SEEK TOTAL TIME : " + totalTime2);
  }

  void generateKeys() {
    for (int i = 0; i < buckets; i++) {
      keySets.add(new ArrayList<>());
    }
    int counter = 0;
    int recordsPerBucket = totalRecords / buckets;
    while (counter < totalRecords) {
      String key = UUID.randomUUID().toString();
      int bucketIndex = Math.abs(key.hashCode()) % buckets;
      if (keySets.get(bucketIndex).size() < recordsPerBucket) {
        keySets.get(bucketIndex).add(key);
        counter++;
      }
    }
    for (int i = 0; i < buckets; i++) {
      Collections.sort(keySets.get(i));
    }
  }
}
