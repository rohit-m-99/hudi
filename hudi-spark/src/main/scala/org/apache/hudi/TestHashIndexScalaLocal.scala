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

import java.util
import java.util.UUID

import org.apache.hudi.hfile.index.{FileSystemTestUtils, HashBasedRecordIndex}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters

object TestHashIndexScalaLocal extends Serializable {

  var iterationCount = 0
  var partitionCount = 1

  def main(args: Array[String]): Unit = {

    println("Starting MAIN " + args.mkString(" "))

    val master = "local[1]"
    val appName = "Bloom Filter trial"
    println("Initializing spark context ")
    // val sparkContext = new SparkContext(master, appName, sparkConf)
    //  val spark = SparkSession.builder()
    //  .appName(appName)
    //.master(master)
    // .config(sparkConf)

    // local set up
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sparkContext = new SparkContext(master, "Local trial", sparkConf)

    /*  val spark = SparkSession.builder
         .appName("Hoodie Datasource test")
         .master("local[3]")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate*/

    val totalEntriesToGenerate: Integer = if (args.length > 0) args(0).toInt else 10
    val numBuckets: Integer = if (args.length > 1) args(1).toInt else 1

    println("Spark context Initialization complete. numKeys " + totalEntriesToGenerate + ", numBuckets " + numBuckets)

    var keyList: Seq[String] = scala.collection.immutable.List()
    var globalEntryMap = new util.HashMap[String, Tuple2[String, String]]
    val basePathStr = FileSystemTestUtils.getRandomPath.toString
    System.out.println("Base path " + basePathStr)
    val index = new HashBasedRecordIndex(numBuckets, basePathStr)
    val randomEntries = generateRandomEntries(totalEntriesToGenerate)
    val keys = sparkContext.parallelize(convertListToSeq(randomEntries)).map(entry => entry._1).collect().toList
    // keys.foreach(key => println("Key :: " + key))
    keyList :+ keys
    val jsc = JavaSparkContext.fromSparkContext(sparkContext)
    //  printBucketMapping(keys, sparkContext, numBuckets)

    convertListToSeq(randomEntries).foreach(entry => globalEntryMap.put(entry._1, entry._2))

    index.insertRecords(jsc, randomEntries)
    val actualValue = index.getRecordLocations(jsc, keys, true)
    if (actualValue.isPresent) {
      val actualList = convertListToSeq(actualValue.get)
      System.out.println("Total size " + actualList.size + " total inserted " + totalEntriesToGenerate)
      // actualList.foreach(rec => println("Expected " + globalEntryMap.get(rec._1) + ", Actual " + rec._2))
      actualList.foreach(rec => assert(globalEntryMap.get(rec._1) == rec._2))
    }
  }

  def convertListToSeq(inputList: java.util.List[Tuple2[String, Tuple2[String, String]]]): Seq[Tuple2[String, Tuple2[String, String]]]
  = JavaConverters.asScalaIteratorConverter(inputList.iterator).asScala.toSeq

  implicit def arrayToList[A](a: Array[A]) = a.toList

  def generateRandomEntries(n: Int): util.List[Tuple2[String, Tuple2[String, String]]] = {
    val toReturn: util.List[Tuple2[String, Tuple2[String, String]]] = new util.ArrayList[Tuple2[String, Tuple2[String, String]]]
    var i: Int = 0
    while (i < n) {
      val key: String = UUID.randomUUID.toString
      val value: String = "value" + key
      toReturn.add(new Tuple2[String, Tuple2[String, String]](key, new Tuple2[String, String](value, value)))
      i += 1
    }
    toReturn
  }

  def printBucketMapping(keys: util.List[String], sc: SparkContext, numBuckets: Integer): Unit = {

    val pairs = sc.parallelize(keys).map(entry => Tuple2((entry.hashCode % numBuckets), entry)).groupByKey()

    val bucketMapping: collection.Map[Int, Iterable[String]] = pairs.collectAsMap()
    import scala.collection.JavaConversions._
    for (entry <- bucketMapping.entrySet) {
      var counter: Int = 0
      val itr: util.Iterator[String] = entry.getValue.iterator
      while (itr.hasNext) {
        counter += 1
        itr.next
      }
      System.out.println("Bucket index " + entry.getKey + " total keys " + counter)
    }
  }

}
