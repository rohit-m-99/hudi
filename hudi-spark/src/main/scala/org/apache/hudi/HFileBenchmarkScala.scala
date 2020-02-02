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

package org.apache.hudi

import java.io.{File, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hudi.hfile.index.{FileSystemTestUtils, HFileUtils}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random


object HFileBenchmarkScala extends Serializable {

  var iterationCount = 0
  var partitionCount = 1

  def main(args: Array[String]): Unit = {

    println("Starting MAIN " + args.mkString(" "))
    val sparkConf = new SparkConf().setAppName("Hoodie data source test")
    sparkConf.set("spark.yarn.executor.memoryOverhead", "3072")
    sparkConf.set("spark.yarn.driver.memoryOverhead", "3072")
    sparkConf.set("spark.task.cpus", "1")
    sparkConf.set("spark.executor.cores", "1")
    sparkConf.set("spark.task.maxFailures", "50")
    sparkConf.set("spark.memory.fraction", "0.4")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("spark.kryoserializer.buffer.max", "512m")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.memory.storageFraction", "0.1")
    sparkConf.set("spark.shuffle.service.enabled", "true")
    sparkConf.set("spark.sql.hive.convertMetastoreParquet", "false")
    sparkConf.set("spark.ui.port", "5555")
    sparkConf.set("spark.driver.maxResultSize", "6g")
    sparkConf.set("spark.executor.heartbeatInterval", "120s")
    sparkConf.set("spark.network.timeout", "600s")
    sparkConf.set("spark.eventLog.overwrite", "true")
    sparkConf.set("spark.eventLog.enabled", "false")
    sparkConf.set("spark.eventLog.dir", "hdfs://ns-platinum-prod-phx/user/spark/applicationHistory")
    sparkConf.set("spark.yarn.historyServer.address", "http://hadoopcopperrm02-phx2.prod.uber.internal:18088")
    // sparkConf.set("spark.eventLog.dir","hdfs:///user/spark/applicationHistory")
    // sparkConf.set("spark.yarn.historyServer.address","http://hadoopneonrm01-dca1:18088")
    sparkConf.set("spark.yarn.max.executor.failures", "100")
    sparkConf.set("spark.sql.catalogImplementation", "hive")
    sparkConf.set("spark.sql.shuffle.partitions", "10")
    sparkConf.set("spark.driver.extraJavaOptions", "-agentlib:hprof=cpu=samples,interval=20,depth=3 -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump-driver.hprof")
    sparkConf.set("spark.executor.extraJavaOptions", "-agentlib:hprof=cpu=samples,interval=20,depth=3 -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump_executor-1.hprof")

    val master = "local[2]"
    val appName = "Bloom Filter trial"
    println("Initializing spark context ")
    val sparkContext = new SparkContext(master, appName, sparkConf)
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

    val numKeys: Integer = if (args.length > 0) args(0).toInt else 10
    val numBuckets: Integer = if (args.length > 1) args(1).toInt else 1
    val keysToReadPerBucket: Integer = if (args.length > 2) args(2).toInt else 10
    val minBlockSize: Integer = if (args.length > 3) args(3).toInt else 1024
    val deleteOnExit: Boolean = if (args.length > 4) args(4).toBoolean else false
    val useSeekToRead: Boolean = if (args.length > 5) args(5).toBoolean else false
    val itrCount: Integer = if (args.length > 6) args(6).toInt else 1
    val partCount: Integer = if (args.length > 7) args(7).toInt else 1
    this.iterationCount = itrCount
    this.partitionCount = partCount

    println("Spark context Initialization complete. numKeys " + numKeys + ", numBuckets " + numBuckets
      + ", keys to read per bucket " + keysToReadPerBucket + ", minBlockSize " + minBlockSize + ", delete On exit " + deleteOnExit)
    val basePath = FileSystemTestUtils.getRandomPath
    val hudiBasePath = new Path(basePath + "/hudi_read")
    this.generateKeys(numKeys, keysToReadPerBucket, minBlockSize, hudiBasePath, deleteOnExit, basePath, sparkContext,
      useSeekToRead)
  }

  implicit def arrayToList[A](a: Array[A]) = a.toList

  @throws[IOException]
  def generateKeys(numKeys: Int, keysToReadPerBucket: Int, minBlockSize: Int, basePath: Path, deleteOnExit: Boolean,
                   pathToDelete: Path, sc: SparkContext, useSeekToRead: Boolean): Unit = {
    val config: Configuration = new Configuration
    val keyFile: String = "key_file"
    val dataFile: String = "data_file"
    println("Base path " + basePath.toString)
    val fs: FileSystem = basePath.getFileSystem(config)
    try {
      if (fs.exists(basePath)) fs.delete(basePath, true)
      val bucketKeyFile: Path = new Path(basePath + "/" + keyFile + "_0")
      val bucketDataFile: Path = new Path(basePath + "/" + dataFile + "_0")
      println("Bucket KeyFile path " + bucketKeyFile.toString)
      val fout: FSDataOutputStream = bucketKeyFile.getFileSystem(config).create(bucketKeyFile)

      val keys: RDD[String] = sc.parallelize(Seq.fill(numKeys)(java.util.UUID.randomUUID.toString))
      val startTime = System.currentTimeMillis()
      keys.collect().sorted.foreach(key => fout.write((key + "\n").getBytes))
      val endTime = System.currentTimeMillis() - startTime
      println("Keys collected, sorted and written to file in " + endTime)
      fout.close()

      writeToHFile(bucketKeyFile, keysToReadPerBucket, minBlockSize, basePath, sc,
        config)

      // look up
      val inputFile = sc.textFile(bucketKeyFile.toString)
      val keysFromFile: Array[String] = inputFile.collect()
      println("Total keys read from file " + keysFromFile.length + ". going to shuffle ")
      val keysList: Seq[String] = arrayToList(keysFromFile)
      val bucketDataFileStr = bucketDataFile.toString


      /*  val shuffledList = Random.shuffle(arrayToList(keysFromFile))
        println("Shuffle complete " + keysFromFile.length)
        val keysToRead: List[String] = shuffledList.take(keysToReadPerBucket).toList
        println("Trimmed keys to read per bucket " + keysToReadPerBucket)

       */

      /*  val parallelKeys = sc.parallelize(keysList)

        var startTime1: Long = System.currentTimeMillis()
        parallelKeys.repartition(partitionCount).foreachPartition(keys => {
          println("Within for each partition :: ")
          val keyList = keys.toList
          readFromHFile(keysList, bucketDataFileStr, keys.size,
            useSeekToRead)
        }
        )
        var totalTime = System.currentTimeMillis() - startTime1
        println("Total time for diff slice reads :: " + (totalTime))
        startTime1 = System.currentTimeMillis()
  */
      println("Diff partition query complete. Reading one full set ")
      readFromHFile(keysList, bucketDataFileStr, keysToReadPerBucket,
        useSeekToRead)
      // totalTime = System.currentTimeMillis() - startTime1
      //println("Total time for full read :: " + (totalTime))
      println("Read benchmark complete")
    } catch {
      case e: Exception =>
        println("Exception thrown " + e.getMessage + " " + e.getCause)
    } finally {
      if (deleteOnExit) {
        println("Delete on exit set to true " + pathToDelete)
        val dir: File = new File(pathToDelete.toString)
        if (dir.exists) {
          println("Cleaning dir " + dir.toString)
          FileSystemTestUtils.deleteDir(dir)
        }
      } else {
        println("Delete on exit set to false " + pathToDelete)
      }
    }
  }

  def writeToHFile(bucketKeyFile: Path, keysToReadPerBucket: Int, minBlockSize: Int, basePath: Path,
                   sc: SparkContext, config: Configuration): Unit = {
    // read
    val dataFile: String = "data_file"
    val inputFile: JavaRDD[String] = sc.textFile(bucketKeyFile.toString)
    val keysFromFile = inputFile.collect
    println("Keys from file size " + keysFromFile.size())
    val bucketDataFile: Path = new Path(basePath + "/" + dataFile + "_0")
    println("Bucket DataFile path " + bucketDataFile.toString)
    HFileUtils.writeToHFile(config, bucketDataFile, keysFromFile, minBlockSize)
    println("Completed write to Bucket DataFile")
  }

  def readFromHFile(keysFromFile: Seq[String], bucketDataFileStr: String, keysToReadPerBucket: Int,
                    useSeekToRead: Boolean): Unit = {
    //  Collections.sort(keysToRead)
    println("Within read from HFile :: ")
    val config: Configuration = new Configuration()
    var timers = new ListBuffer[Long]()
    val bucketDataFile = new Path(bucketDataFileStr)
    println("Total keys from file " + keysFromFile.size + ", keys to read " + keysToReadPerBucket)
    for (i <- 1 to iterationCount) {
      val shuffledList = Random.shuffle(keysFromFile)
      val keysToRead: List[String] = shuffledList.take(keysToReadPerBucket).toList
      val startTime: Long = System.currentTimeMillis
      if (!useSeekToRead) {
        HFileUtils.readAllRecordsSequentially(bucketDataFile, config, HFileUtils.getKeys(keysToRead))
      } else {
        HFileUtils.readAllRecordsWithSeek(bucketDataFile, config, HFileUtils.getKeys(keysToRead))
      }
      val totalTime: Long = System.currentTimeMillis - startTime
      timers += totalTime
    }
    println("Total time for look up " + timers.toList)
    println("Percentiles :: 0.25 " + percentiles(timers, 0.25) + ", 0.5 " + percentiles(timers, 0.5)
      + ", 0.75 " + percentiles(timers, 0.75) + ", 0.9 " + percentiles(timers, 0.9) + ", 0.95 "
      + percentiles(timers, 0.95) + ", 0.99 " + percentiles(timers, 0.99))
  }

  def percentiles(latencies: ListBuffer[Long], percentile: Double): Long = {
    latencies.sorted
    val Index: Int = Math.ceil(percentile * latencies.size).toInt
    latencies.get(Index - 1)
  }
}
