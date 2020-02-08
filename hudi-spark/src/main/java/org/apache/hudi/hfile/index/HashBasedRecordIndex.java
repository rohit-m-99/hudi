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

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import scala.Tuple2;

public class HashBasedRecordIndex implements HudiRecordIndex, Serializable {

  HFileBucket[] buckets;
  int numBuckets;
  String basePath;

  public HashBasedRecordIndex(int numBuckets, String basePath) {
    this.numBuckets = numBuckets;
    this.basePath = basePath;
    buckets = new HFileBucket[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      buckets[i] = new HFileBucket(getBucketFileName(i));
    }
  }

  private String getBucketFileName(int index) {
    return basePath + "/bucket_" + index;
  }

  @Override
  public Optional<List<Tuple2<String, Tuple2<String, String>>>> getRecordLocations(JavaSparkContext jsc, List<String> recordKeys, boolean scan) {
    try {
      JavaPairRDD pairRDD = jsc.parallelize(recordKeys).mapToPair(entry -> new Tuple2<>(entry, entry));
      JavaPairRDD partitionedRDD = pairRDD.partitionBy(new HashPartitioner(numBuckets));

      JavaRDD resultRDD = partitionedRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, String>>, Iterator<Tuple2<String, Tuple2<String, String>>>>() {
        @Override
        public Iterator<Tuple2<String, Tuple2<String, String>>> call(Integer v1, Iterator<Tuple2<String, String>> v2) throws Exception {
          List<String> keysToLookup = new ArrayList<String>();
          while (v2.hasNext()) {
            keysToLookup.add(v2.next()._1);
          }
          System.out.println("Partition index " + v1 + " total values " + keysToLookup.size());
          return buckets[(int) v1.longValue()].getRecordLocation(keysToLookup, scan).iterator();
        }
      }, true);

      List resultList = resultRDD.collect();
      return Optional.of(resultList);
    } catch (Exception e) {
      System.out.println("Get record locations. Exception thrown " + e.getCause() + " " + e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean insertRecords(JavaSparkContext jsc, List<Tuple2<String, Tuple2<String, String>>> records) {
    try {
      JavaPairRDD recordToPartitionPathFileIdPairRDD = jsc.parallelize(records).mapToPair(entry -> new Tuple2<>(entry._1, entry._2));
      JavaPairRDD<String, Tuple2<String, String>> sortedPairRDD = recordToPartitionPathFileIdPairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(numBuckets));

      JavaRDD<Boolean> resultRDD = sortedPairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Tuple2<String, String>>>, Iterator<Boolean>>() {
        @Override
        public Iterator<Boolean> call(Integer v1, Iterator<Tuple2<String, Tuple2<String, String>>> v2) throws Exception {
          // todo: check if single line to fetch all records from iterator
          List<Tuple2<String, Tuple2<String, String>>> entries = new ArrayList<>();
          while (v2.hasNext()) {
            entries.add(v2.next());
          }
          System.out.println("Partition index " + v1 + " total values " + entries.size() + " bucket index " + ((int) v1.longValue()));
          return Collections.singletonList(buckets[(int) v1.longValue()].insertRecords(entries)).iterator();
        }
      }, true);

      return resultRDD.reduce((a, b) -> (a && b));
    } catch (Exception e) {
      System.out.println("Insert records. Exception thrown " + e.getCause() + " " + e.getMessage());
      throw e;
    }
  }
}
