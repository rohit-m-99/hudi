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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HFileBenchmarkJava implements Serializable {
  public static void main(String[] args) throws IOException {

    System.out.println("Starting MAIN " + Arrays.toString(args));
    SparkConf sparkConf = new SparkConf().setAppName("Hoodie data source test");
    sparkConf.set("spark.yarn.executor.memoryOverhead", "3072");
    sparkConf.set("spark.yarn.driver.memoryOverhead", "3072");
    sparkConf.set("spark.task.cpus", "1");
    sparkConf.set("spark.executor.cores", "1");
    sparkConf.set("spark.task.maxFailures", "50");
    sparkConf.set("spark.memory.fraction", "0.4");
    sparkConf.set("spark.rdd.compress", "true");
    sparkConf.set("spark.kryoserializer.buffer.max", "512m");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.memory.storageFraction", "0.1");
    sparkConf.set("spark.shuffle.service.enabled", "true");
    sparkConf.set("spark.sql.hive.convertMetastoreParquet", "false");
    sparkConf.set("spark.ui.port", "5555");
    sparkConf.set("spark.driver.maxResultSize", "6g");
    sparkConf.set("spark.executor.heartbeatInterval", "120s");
    sparkConf.set("spark.network.timeout", "600s");
    sparkConf.set("spark.eventLog.overwrite", "true");
    sparkConf.set("spark.eventLog.enabled", "false");


    sparkConf.set("spark.yarn.max.executor.failures", "100");
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.shuffle.partitions", "10");
    sparkConf.set("spark.driver.extraJavaOptions", "-XX:+PrintTenuringDistribution -XX:+PrintGCDetails "
        + "-XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime "
        + "-XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump-driver.hprof");
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc "
        + "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions"
        + " -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump_executor-1.hprof");

    String master = "yarn";
    String appName = "Bloom Filter trial";
    System.out.println("Initializing spark context ");
    //SparkContext sparkContext = new SparkContext(master, appName, sparkConf);
    JavaSparkContext jsc = new JavaSparkContext(master, appName, sparkConf);
    //  val spark = SparkSession.builder()
    //  .appName(appName)
    //.master(master)
    // .config(sparkConf)

    // local set up
    /* val sparkConf = new SparkConf().setAppName("Hoodie data source test")
    val sparkContext = new SparkContext(appName, "local[3]", sparkConf)*/

    /*  val spark = SparkSession.builder
         .appName("Hoodie Datasource test")
         .master("local[3]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate*/

    System.out.println("Spark context Initialization complete ");

    // val globalSet = sparkContext.parallelize(Seq.fill(totalSize)(java.util.UUID.randomUUID.toString))

    int numKeys = 10;
    int numBuckets = 1;
    int keysToReadPerBucket = 10;
    int minBlockSize = 1024;
    if (args.length > 0) {
      numKeys = Integer.parseInt(args[0]);
    }
    if (args.length > 1) {
      numBuckets = Integer.parseInt(args[1]);
    }
    if (args.length > 2) {
      keysToReadPerBucket = Integer.parseInt(args[2]);
    }
    if (args.length > 3) {
      minBlockSize = Integer.parseInt(args[3]);
    }


    HFileBenchmarkJava benchmarkSpark = new HFileBenchmarkJava();
    Path basePath = FileSystemTestUtils.getRandomPath();
    Path hudiBasePath = new Path(basePath + "/hudi_read");
    benchmarkSpark.generateKeys(numKeys, numBuckets, keysToReadPerBucket, minBlockSize, hudiBasePath, false, basePath, jsc);
  }

  public static void generateKeys(int numKeys, int buckets, int keysToReadPerBucket, int minBlockSize,
                                  Path basePath, boolean deleteOnExit, Path pathToDelete,
                                  JavaSparkContext jsc) throws IOException {
    Configuration config = new Configuration();
    String keyFile = "key_file";
    String dataFile = "data_file";
    System.out.println("Base path " + basePath.toString());
    FileSystem fs = basePath.getFileSystem(config);

    try {
      if (fs.exists(basePath)) {
        fs.delete(basePath, true);
      }
      Path bucketKeyFile = new Path(basePath + "/" + keyFile + "_0");
      System.out.println("Bucket KeyFile path " + bucketKeyFile.toString());
      FSDataOutputStream fout = bucketKeyFile.getFileSystem(config).create(bucketKeyFile);
      List<String> keys = IntStream.range(0, numKeys)
          .mapToObj(i -> UUID.randomUUID().toString())
          .collect(Collectors.toList());

      keys.forEach(key -> {
        try {
          fout.write(key.getBytes());
        } catch (IOException e) {
          e.printStackTrace();
        }
      });
      fout.close();

      // read
      JavaRDD<String> inputFile = jsc.textFile(bucketKeyFile.toString());
      // JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));
      List<String> keysFromFile = inputFile.collect();
      System.out.println("Keys read from file " + Arrays.toString(keysFromFile.toArray()));

      Collections.shuffle(keysFromFile);
      List<String> keysToRead = keysFromFile.subList(0, keysToReadPerBucket);
      Collections.sort(keysToRead);
      System.out.println("Keys to be read " + Arrays.toString(keysFromFile.toArray()));
      Path bucketDataFile = new Path(basePath + "/" + dataFile + "_0");
      System.out.println("Bucket DataFile path " + bucketDataFile.toString());
      HFileUtils.writeToHFile(config, bucketDataFile, keysToRead, minBlockSize);

      // look up
      Collections.sort(keysFromFile);
      long startTime = System.currentTimeMillis();
      HFileUtils.readAllRecordsSequentially(bucketDataFile, config, HFileUtils.getKeys(keysFromFile));
      long totalTime = (System.currentTimeMillis() - startTime);
      System.out.println("Total time for look up " + totalTime);


    } catch (Exception e) {
      System.err.println("Exception thrown " + e.getMessage() + " " + e.getCause());
    } finally {
      if (deleteOnExit) {
        File dir = new File(pathToDelete.toString());
        if (dir.exists()) {
          System.out.println("Cleaning dir " + dir.toString());
          FileSystemTestUtils.deleteDir(dir);
        }
      }
    }
  }
}
