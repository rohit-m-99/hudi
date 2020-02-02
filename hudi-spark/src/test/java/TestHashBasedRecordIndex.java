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

import org.apache.hudi.hfile.index.FileSystemTestUtils;
import org.apache.hudi.hfile.index.HashBasedRecordIndex;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import scala.Tuple2;

public class TestHashBasedRecordIndex {

  List<String> keyList;
  Map<String, Tuple2<String, String>> globalEntryMap;
  String basePathStr;
  JavaSparkContext jsc;
  int numBuckets = 1;

  public TestHashBasedRecordIndex() {
    keyList = new ArrayList<>();
    globalEntryMap = new HashMap<>();
    basePathStr = FileSystemTestUtils.getRandomPath().toString();
    /*  SparkSession sparkSession = SparkSession.builder()
        .appName("Hoodie Datasource test")
        .master("local[3]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
    */

    SparkConf conf = new SparkConf().setAppName("Hoodie Datasource test").setMaster("local[3]");
    jsc = new JavaSparkContext(conf);
  }

  @Test
  public void sampleTest() {
    int totalEntriesToGenerate = 10;
    System.out.println("Base path " + basePathStr);
    HashBasedRecordIndex index = new HashBasedRecordIndex(numBuckets, basePathStr);
    List<Tuple2<String, Tuple2<String, String>>> randomEntries = generateRandomEntries(totalEntriesToGenerate);
    List<String> keys = jsc.parallelize(randomEntries).map(entry -> entry._1).collect();
    keyList.addAll(keys);
    //printBucketMapping(keys);
    randomEntries.forEach(entry -> globalEntryMap.put(entry._1, entry._2));

    index.insertRecords(jsc, randomEntries);

    Optional<List<Tuple2<String, Tuple2<String, String>>>> actualValue = index.getRecordLocations(jsc, keys, true);
    if (actualValue.isPresent()) {
      List<Tuple2<String, Tuple2<String, String>>> actualList = actualValue.get();
      System.out.println("Total size " + actualList.size() + " total inserted " + totalEntriesToGenerate);
      Assert.assertEquals(totalEntriesToGenerate, actualList.size());
      actualList.forEach(entry -> Assert.assertEquals(globalEntryMap.get(entry._1), entry._2));
    }
  }

  private List<Tuple2<String, Tuple2<String, String>>> generateRandomEntries(int n) {
    List<Tuple2<String, Tuple2<String, String>>> toReturn = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String key = UUID.randomUUID().toString();
      String value = "value" + key;
      toReturn.add(new Tuple2<>(key, new Tuple2<>(value, value)));
    }
    return toReturn;
  }

  private void printBucketMapping(List<String> keys) {
    // JavaPairRDD<String, String> entries = jsc.parallelize(keys).mapToPair(entry -> new Tuple2<>(entry, entry)).partitionBy(new HashPartitioner(numBuckets));
    // entries.mapPartitionsWithIndex()

    JavaPairRDD<Integer, Iterable<String>> pairs = jsc.parallelize(keys).mapToPair(entry -> new Tuple2(entry.hashCode() % numBuckets, entry)).groupByKey();
    Map<Integer, Iterable<String>> bucketMapping = pairs.collectAsMap();
    for (Map.Entry<Integer, Iterable<String>> entry : bucketMapping.entrySet()) {
      int counter = 0;
      Iterator<String> itr = entry.getValue().iterator();
      while (itr.hasNext()) {
        counter++;
        itr.next();
      }
      System.out.println("Bucket index " + entry.getKey() + " total keys " + counter);
    }
  }
}
