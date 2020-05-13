/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import java.util.UUID

import org.apache.hudi.common.bloom.{BloomFilter, BloomFilterFactory, BloomFilterTypeCode}
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode}
import org.openjdk.jmh.infra.Blackhole

class MyBenchmark {

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def test(blackHole: Blackhole): Unit = {
    val size = 100
    // inputs = new ArrayList<>();
    val filter = getBloomFilter(BloomFilterTypeCode.SIMPLE.name, size, 0.000001, size * 10)
    for (i <- 0 until size) {
      val key = UUID.randomUUID.toString
      //inputs.add(key);
      filter.add(key)
    }

    val serString = filter.serializeToString
    val recreatedBloomFilter = BloomFilterFactory.fromString(serString, BloomFilterTypeCode.SIMPLE.name)
    blackHole.consume(recreatedBloomFilter)
  }

  def getBloomFilter(typeCode: String, numEntries: Int, errorRate: Double, maxEntries: Int): BloomFilter = {
    if (typeCode.equalsIgnoreCase(BloomFilterTypeCode.SIMPLE.name)) {
      BloomFilterFactory.createBloomFilter(numEntries, errorRate, -(1), typeCode)
    }
    else {
      BloomFilterFactory.createBloomFilter(numEntries, errorRate, maxEntries, typeCode)
    }
  }

}
