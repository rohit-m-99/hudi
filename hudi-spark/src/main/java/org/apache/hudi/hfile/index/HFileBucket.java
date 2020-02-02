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

package org.apache.hudi.hfile.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class HFileBucket implements Serializable {

  List<String> hFilePaths;
  String basePath;
  final String filePrefix = "filePrefix";

  HFileBucket(String basePath) {
    this.basePath = basePath;
    this.hFilePaths = new ArrayList<>();
  }

  public synchronized List<Tuple2<String, Tuple2<String, String>>> getRecordLocation(List<String> recordKeys, boolean scan) throws IOException {
    System.out.println("Get reocrd locations in HFileBucket with base path " + basePath.toString());
    hFilePaths.clear();
    File dir = new File(basePath);
    if (dir.exists()) {
      File[] files = dir.listFiles((dir1, name) -> {
        System.out.println("Get record locations. checking for file name to filter " + name);
        return name.startsWith("file") && !name.endsWith("crc");
      });
      for (File file : files) {
        System.out.println("Adding file path to list of hFile paths " + file.getAbsolutePath());
        hFilePaths.add(file.getAbsolutePath());
      }
    } else {
      System.out.println("Dir does not exist " + basePath.toString());
    }

    // JavaRDD<Path> pathJavaRDD = jsc.parallelize(hFilePaths);

    // List<Tuple2<String, Tuple2<String, String>>> toReturn = new ArrayList<>();
    //  List<JavaRDD<Tuple2<String, Tuple2<String, String>>>> toReturn = pathJavaRDD.map(path -> getRecordLocations(path, jsc, recordKeys)).collect(Collectors.joining());
    //JavaRDD<JavaRDD<Tuple2<String, Tuple2<String, String>>>> returnVal = jsc.union(jsc.parallelize(toReturn));

    List<Tuple2<String, Tuple2<String, String>>> toReturn = new ArrayList<>();
    // hFilePaths.foreach(path -> toReturn.addAll(getRecordLocations(path, recordKeys)));
    for (String path : hFilePaths) {
      toReturn.addAll(getRecordLocations(new Path(path), recordKeys, scan));
    }

    //List<JavaRDD<Tuple2<String, Tuple2<String, String>>>> toReturn = pathJavaRDD.map(path -> getRecordLocations(jsc, path, recordKeys)).collect();

    // JavaRDD<JavaRDD<Tuple2<String, Tuple2<String, String>>>> toReturn = pathJavaRDD.flatMap(path ->
    //  getRecordLocations(jsc, path, recordKeys).collect().stream()).collect();


    return toReturn;
  }

  private synchronized List<Tuple2<String, Tuple2<String, String>>> getRecordLocations(Path path, List<String> recordKeys, boolean scan) throws IOException {
    System.out.println("Starting to fetch record locations for " + path.toString());
    org.apache.hadoop.conf.Configuration conf = new Configuration();
    // conf.set(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, "true");
    // conf.set("CACHE_DATA_IN_L1","true");
    // conf.set("hbase.hfile.drop.behind.compaction", "false");
    CacheConfig cacheConf = new CacheConfig(conf);
    //  cacheConf.setCacheDataInL1(true);
    // cacheConf.setEvictOnClose(false);
    FileSystem fileSystem = path.getFileSystem(conf);
    List<Tuple2<String, Tuple2<String, String>>> toReturn = new ArrayList<>();

    if (scan) {
      HFile.Reader reader = HFile.createReader(fileSystem, path, cacheConf, conf);
      // Load up the index.
      reader.loadFileInfo();
      // Get a scanner that caches and that does not use pread.
      HFileScanner scanner = reader.getScanner(true, false);
      // Align scanner at start of the file.
      scanner.seekTo();
      Map<String, String> keyValuePairs = readAllRecords(scanner);
      reader.close();
      int counter = 0;
      boolean notFound = false;
      for (String key : recordKeys) {
        byte[] keyBytes = HFileUtils.getSomeKey(key);
        if (keyValuePairs.containsKey(Bytes.toString(keyBytes))) {
          counter++;
          toReturn.add(new Tuple2(key, keyValuePairs.get(Bytes.toString(keyBytes))));
        } else {
          notFound = true;
          System.out.println("Key not found " + key);
        }
      }
      System.out.println("Found " + counter + " keys when read fully in " + path.toString());
      System.out.println("Total keys found for " + path.toString() + " : " + keyValuePairs.entrySet().toString());
      return toReturn;
    } else {

      // System.out.println("Keys to be looked up with " + path.toString() +" " + Arrays.toString(recordKeys.toArray()));
      HFile.Reader reader = HFile.createReader(fileSystem, path, cacheConf, conf);
      // Load up the index.
      reader.loadFileInfo();
      // Get a scanner that caches and that does not use pread.
      HFileScanner scanner = reader.getScanner(true, true);
      scanner.seekTo();
      for (String key : recordKeys) {
        byte[] keyToLookup = HFileUtils.getSomeKey(key);
        if (scanner.seekTo(KeyValue.createKeyValueFromKey(keyToLookup)) == 0) {
          ByteBuffer readKey = scanner.getKey();
          ByteBuffer readValue = scanner.getValue();
          String value = Bytes.toString(Bytes.toBytes(readValue));
          toReturn.add(new Tuple2<>(key, new Tuple2<>(value, value)));
        } else {
          System.out.println("Entry not foud for " + key + " in " + path.toString());
        }
      }
      reader.close();
      System.out.println("Returning " + toReturn.size() + " entries from " + path.toString() + ". Total passed in " + recordKeys.size());
      return toReturn;
    }
  }

  private static Map<String, String> readAllRecords(HFileScanner scanner) throws IOException {
    return readAndCheckbytes(scanner);
  }

  // read the records and check
  private static Map<String, String> readAndCheckbytes(HFileScanner scanner)
      throws IOException {
    Map<String, String> keyValuePairs = new HashMap<>();
    while (true) {
      ByteBuffer key = scanner.getKey();
      ByteBuffer val = scanner.getValue();
      keyValuePairs.put(Bytes.toString(Bytes.toBytes(key)), Bytes.toString(Bytes.toBytes(val)));
      if (!scanner.next()) {
        break;
      }
    }
    return keyValuePairs;
  }

  public synchronized boolean insertRecords(List<Tuple2<String, Tuple2<String, String>>> records) {

    hFilePaths.clear();
    File dir = new File(basePath);
    System.out.println("Insert record locations. Base path " + basePath.toString());
    if (dir.exists()) {
      File[] files = dir.listFiles((dir1, name) -> {
        return name.startsWith("file") && !name.endsWith("crc");
      });
      for (File file : files) {
        System.out.println("Insert record locations. Adding file to hFile paths " + file.getAbsolutePath());
        hFilePaths.add(file.getAbsolutePath());
      }
    } else {
      System.out.println("Insert record locations. Base path does not exist " + basePath.toString());
    }

    Path path = getNewHFilePath();
    System.out.println("Apending to a new file " + path.toString() + ". Total entries " + records.size());
    org.apache.hadoop.conf.Configuration conf = new Configuration();
    try {
      CacheConfig cacheConf = new CacheConfig(conf);
      FSDataOutputStream fout = createFSOutput(path, conf);
      HFileContext meta = new HFileContextBuilder()
          .withBlockSize(1024)
          .build();
      HFile.Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(fout)
          .withFileContext(meta)
          .withComparator(new KeyValue.KVComparator())
          .create();

      List<String> keys = new ArrayList<>();
      for (int i = 0; i < records.size(); i++) {
        Tuple2<String, Tuple2<String, String>> entry = records.get(i);
        String key = entry._1;
        keys.add(key);
        String value = entry._2._1;
        KeyValue kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("family"), Bytes.toBytes("qual"),
            Bytes.toBytes(value));
        writer.append(kv);
      }
      writer.close();
      fout.close();
      hFilePaths.add(path.toString());
      System.out.println("Keys to written to " + path.toString() + " " + Arrays.toString(keys.toArray()));

      File dir1 = new File(basePath);
      if (dir1.exists()) {
        File[] files = dir.listFiles((dir2, name) -> {
          System.out.println("Insert records. checking for file name to filter after inserting " + name);
          return name.startsWith("file") && !name.endsWith("crc");
        });
        for (File file : files) {
          System.out.println("Insert records. Files fetched after insertion " + file.getAbsolutePath());
        }
      } else {
        System.out.println("Insert records. Dir does not exist after insertion " + basePath.toString());
      }

      return true;
    } catch (Exception e) {
      System.err.println("Exception thrown while writing to HFile " + path);
      e.printStackTrace();
    }
    return false;
  }

  private static FSDataOutputStream createFSOutput(Path name, Configuration conf) throws IOException {
    //if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = name.getFileSystem(conf).create(name);
    return fout;
  }

  Path getNewHFilePath() {
    return new Path(basePath + "/" + filePrefix + hFilePaths.size());
  }
}
