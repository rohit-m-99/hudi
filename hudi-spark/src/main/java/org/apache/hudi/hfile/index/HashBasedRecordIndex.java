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
          List<String> result = new ArrayList<String>();
          while (v2.hasNext()) {
            result.add(v2.next()._1);
          }
          System.out.println("Partition index " + v1 + " total values " + result.size());
          return buckets[(int) v1.longValue()].getRecordLocation(result, scan).iterator();
        }
      }, true);

      List sumRDD = resultRDD.collect();

      return Optional.of(sumRDD);
    } catch (Exception e) {
      System.out.println("Get record locations. Exception thrown " + e.getCause() + " " + e.getMessage());
      throw e;
    }

    /*
    Map<Integer, Iterator<String>> sortedPairMap = sortedPairRDD.collectAsMap();
    for(Map.Entry<Integer, Iterator<String>> entry: sortedPairMap.entrySet()){
      List <String> result = new ArrayList <String> ();
      Iterator<String> itr = entry.getValue();
      while (itr.hasNext ()) result.add(itr.next());
      System.out.println("Entry " +entry.getKey()+" value size "+ result.size());
    }




    JavaPairRDD<Long, Iterable<String>> groupByvals = jsc.parallelize(recordKeys).groupBy(key -> (((key.hashCode() + 2147483648L) % 2147481648L)%numBuckets));

    // List<Tuple2<String, Tuple2<String, String>>> toReturn = new ArrayList<>();

    /*groupByvals.foreach(entry ->  {
      List <String> result = new ArrayList <String> ();
      Iterator<String> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
      toReturn.addAll(buckets[entry._1].getRecordLocation(result));
    });
    return Optional.of(toReturn);*/


    /*
    List<Tuple2<String, Tuple2<String, String>>> toReturn = groupByvals.flatMap(new FlatMapFunction<Tuple2<Long, Iterable<String>>, Tuple2<String, Tuple2<String, String>>>() {


      @Override
      public Iterator<Tuple2<String, Tuple2<String, String>>> call(Tuple2<Long, Iterable<String>> integerIterableTuple2) throws Exception {
        List <String> result = new ArrayList <String> ();
        Iterator<String> itr = integerIterableTuple2._2.iterator();
        while (itr.hasNext ()) result.add(itr.next());
        System.out.println("Entries to be looked up in bucket index " + integerIterableTuple2._1.longValue() +" -> "+ result.size());
        return buckets[(int)integerIterableTuple2._1.longValue()].getRecordLocation(result).iterator();
      }
    }).collect();

    return Optional.of(toReturn);

     /* List<List<Tuple2<String, Tuple2<String, String>>>> toReturn = groupByvals.map(entry ->  {
      List <String> result = new ArrayList <String> ();
      Iterator<String> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
      return buckets[entry._1].getRecordLocation(result);}).collect();
    return Optional.empty();*/


    /*JavaRDD<List<Tuple2<String, Tuple2<String, String>>>> toReturn =  groupByvals.map(entry ->  {
      List <String> result = new ArrayList <String> ();
      Iterator<String> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
      return buckets[entry._1].getRecordLocation(jsc, jsc.parallelize(result)).collect();
    });


    //  case (x, y, iter) => (x, y, iter.toArray)

    /* groupByvals.mapToPair(entry -> Tuple2(entry._1, entry._2().forEach(Collectors.toList()));

    groupByvals.map(case (index, iter) => buckets[index].getRecordLocation( iter.toList));

    groupByvals.map(e -> buckets[e._1].getRecordLocation(jsc, e._2.) ).


    x => x.foreach(accum.add(_)))

  /*  jsc.parallelize(recordKeys).groupBy().for

    rdd.foreachPartition(iter -> {
      while (iter.hasNext()) {
        iter.next();
        accum.add(1);
      }
    });

   */


    /* JavaPairRDD pairRDD = jsc.parallelize(recordKeys).mapToPair(str -> new Tuple2(str, str)).partitionBy(new HashPartitioner(numBuckets));


    pairRDD.mapPartitions()


    pairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, JavaRDD<Tuple2<String, Tuple2<String, String>>>>() {
      @Override
      public JavaRDD<Tuple2<String, Tuple2<String, String>>> call(Integer v1, Iterator<String> v2) throws Exception {
        List <String> result = new ArrayList <String> ();
        while (v2.hasNext ()) result.add(v2.next());
        return buckets[v1].getRecordLocation(jsc, jsc.parallelize(result));
      }
    });

    pairRDD.mapPartitionsToPair(
        (new PairFlatMapFunction<Iterator<String>, String, String>() {
          public Iterable <Tuple2 <String, String> > call (Iterator <String> input) {
            List <String> result = new ArrayList<String>();
            while (input.hasNext ()) result.add (doSomeThing (input.next ()));
            return result;
          }
        });
    );


    pairRDD.foreachPartition(iter -> {
      while (iter.hasNext()) {
        iter.next();
        accum.add(1);
      }
    });


    return Optional.empty();

    */
  }

  @Override
  public boolean insertRecords(JavaSparkContext jsc, List<Tuple2<String, Tuple2<String, String>>> records) {
    try {
      JavaPairRDD pairRDD = jsc.parallelize(records).mapToPair(entry -> new Tuple2<>(entry._1, entry._2));
      JavaPairRDD<String, Tuple2<String, String>> sortedPairRDD = pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(numBuckets));

      JavaRDD<Boolean> resultRDD = sortedPairRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Tuple2<String, String>>>, Iterator<Boolean>>() {
        @Override
        public Iterator<Boolean> call(Integer v1, Iterator<Tuple2<String, Tuple2<String, String>>> v2) throws Exception {
          // todo: check if single line to fetch all records from iterator
          List<Tuple2<String, Tuple2<String, String>>> result = new ArrayList<>();
          while (v2.hasNext()) {
            result.add(v2.next());
          }
          System.out.println("Partition index " + v1 + " total values " + result.size() + " bucket index " + ((int) v1.longValue()));
          return Collections.singletonList(buckets[(int) v1.longValue()].insertRecords(result)).iterator();
        }
      }, true);

      return resultRDD.reduce((a, b) -> (a && b));
    } catch (Exception e) {
      System.out.println("Insert records. Exception thrown " + e.getCause() + " " + e.getMessage());
      throw e;
    }


    /*JavaPairRDD<Long, Iterable<Tuple2<String, Tuple2<String, String>>>> groupByvals =
        jsc.parallelize(records).groupBy(key -> ((key._1.hashCode() + 2147483648L) % 2147481648L)%numBuckets);

    List<Tuple2<Long, Iterable<Tuple2<String, Tuple2<String, String>>>>> groupList = groupByvals.collect();
    for(Tuple2<Long, Iterable<Tuple2<String, Tuple2<String, String>>>> entry: groupList) {
      List <Tuple2<String, Tuple2<String, String>>> result = new ArrayList <> ();
      Iterator<Tuple2<String, Tuple2<String, String>>> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
     // System.out.println("Bucket index "+ entry._1+", entry size " +result.size()+ ".  all values " + Arrays.toString(result.toArray()));
    }


    groupByvals.foreach(entry ->  {
      List <Tuple2<String, Tuple2<String, String>>> result = new ArrayList <> ();
      Iterator<Tuple2<String, Tuple2<String, String>>> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
      Collections.sort(result, new Comparator<Tuple2<String, Tuple2<String, String>>>() {
        @Override
        public int compare(Tuple2<String, Tuple2<String, String>> o1, Tuple2<String, Tuple2<String, String>> o2) {
          return o1._1.compareTo(o2._1);
        }
      });
    //  System.out.println(entry._1 + " Buckets size " + buckets.length+". " + Arrays.toString(buckets));
      System.out.println("Entries to be written in bucket index " + entry._1.longValue() +" -> "+ result.size());
      if(!buckets[(int)entry._1.longValue()].insertRecords(result)) {
        System.out.println("Failing to insert into bucket "+ entry._1);
      } else{
        System.out.println("Successfully inserted into bucket "+ entry._1);
      }
    });
    return true;


    /*JavaRDD<Boolean> toReturn =  groupByvals.map(entry ->  {
      List <Tuple2<String, Tuple2<String, String>>> result = new ArrayList <> ();
      Iterator<Tuple2<String, Tuple2<String, String>>> itr = entry._2.iterator();
      while (itr.hasNext ()) result.add(itr.next());
      return buckets[entry._1].insertRecords(jsc, result);
    });

     */
  }
}
