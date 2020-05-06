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

import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarnessJunit5;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hudi.common.HoodieTestDataGenerator.AVRO_SCHEMA_1;
import static org.apache.hudi.common.HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS;

public class TestHudiParquetWriter extends HoodieCommonTestHarnessJunit5 {

  private static final Logger LOG = LoggerFactory.getLogger(TestHudiParquetWriter.class);
  private JavaSparkContext jsc;
  private SQLContext sqlContext;
  private HoodieTestDataGenerator dataGen;
  protected transient FileSystem fs;

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHudiParquetWriter");
    initPath();
    initTestDataGenerator();
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupSparkContexts();
    cleanupFileSystem();
  }

  @Test
  public void simpleTest() {
    String commitTime = "000";
    try {
      List<GenericRecord> records = dataGen.generateInsertsGenRec(commitTime, 10);
      JavaRDD<GenericRecord> javaRDD = jsc.parallelize(records);

      // Trial 1
      /*Configuration config = jsc.hadoopConfiguration();
      config.set("org.apache.spark.sql.parquet.row.attributes", AVRO_SCHEMA_1.toString());
      Dataset<Row> rowDataset = AvroConversionUtils.createDataFrame(javaRDD.rdd(), AVRO_SCHEMA_1.toString(), sqlContext.sparkSession());
      Dataset<Boolean> result = rowDataset.mapPartitions(new HudiParquetWriter(basePath, RowEncoder.apply(rowDataset.schema()),
          new SerializableConfiguration(config)), Encoders.BOOLEAN());

      System.out.println("output 1 " + Arrays.toString(result.collectAsList().toArray()));
      */

      // Trial 2
       JavaRDD<Boolean> result2 = javaRDD.mapPartitions(new HudiParquetWriterJava(basePath, new SerializableConfiguration(jsc.hadoopConfiguration())));
       System.out.println("output 2 " + Arrays.toString(result2.collect().toArray()));
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  protected void initSparkContexts(String appName) {
    // Initialize a local spark env
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(appName));
    jsc.setLogLevel("ERROR");

    // SQLContext stuff
    sqlContext = new SQLContext(jsc);
  }

  protected void initTestDataGenerator() {
    dataGen = new HoodieTestDataGenerator();
  }

  /**
   * Initializes a file system with the hadoop configuration of Spark context.
   */
  protected void initFileSystem() {
    if (jsc == null) {
      throw new IllegalStateException("The Spark context has not been initialized.");
    }

    initFileSystemWithConfiguration(jsc.hadoopConfiguration());
  }

  private void initFileSystemWithConfiguration(Configuration configuration) {
    if (basePath == null) {
      throw new IllegalStateException("The base path has not been initialized.");
    }

    fs = FSUtils.getFs(basePath, configuration);
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  protected void cleanupSparkContexts() {
    if (sqlContext != null) {
      LOG.info("Clearing sql context cache of spark-session used in previous test-case");
      sqlContext.clearCache();
      sqlContext = null;
    }

    if (jsc != null) {
      LOG.info("Closing spark context used in previous test-case");
      jsc.close();
      jsc.stop();
      jsc = null;
    }
  }

  /**
   * Cleanups file system.
   *
   * @throws IOException
   */
  protected void cleanupFileSystem() throws IOException {
    if (fs != null) {
      LOG.warn("Closing file-system instance used in previous test-run");
      fs.close();
    }
  }

}
