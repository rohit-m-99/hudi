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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings;

/**
 * Sample program that writes & reads hoodie tables via the Spark datasource streaming.
 */
public class SivaStreamingApp {

  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "/tmp/hoodie/streaming/sample-table";

  @Parameter(names = {"--streaming-source-path", "-ssp"}, description = "path for streaming source file folder")
  private String streamingSourcePath = "/tmp/hoodie/streaming/source";

  @Parameter(names = {"--streaming-checkpointing-path", "-scp"},
      description = "path for streaming checking pointing folder")
  private String streamingCheckpointingPath = "/tmp/hoodie/streaming/checkpoint";

  @Parameter(names = {"--streaming-duration-in-ms", "-sdm"},
      description = "time in millisecond for the streaming duration")
  private Long streamingDurationInMs = 15000L;

  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
  private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.MERGE_ON_READ.name();

  @Parameter(names = {"--hive-sync", "-hv"}, description = "Enable syncing to hive")
  private Boolean enableHiveSync = false;

  @Parameter(names = {"--hive-db", "-hd"}, description = "hive database")
  private String hiveDB = "default";

  @Parameter(names = {"--hive-table", "-ht"}, description = "hive table")
  private String hiveTable = "hoodie_sample_test";

  @Parameter(names = {"--hive-user", "-hu"}, description = "hive username")
  private String hiveUser = "hive";

  @Parameter(names = {"--hive-password", "-hp"}, description = "hive password")
  private String hivePass = "hive";

  @Parameter(names = {"--hive-url", "-hl"}, description = "hive JDBC URL")
  private String hiveJdbcUrl = "jdbc:hive2://localhost:10000";

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
  private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--generate-only-data"}, description = "generate streaming data")
  private Boolean generateOnlyData = true;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;


  private static final Logger LOG = LogManager.getLogger(SivaStreamingApp.class);

  public static void main(String[] args) throws Exception {
    SivaStreamingApp cli = new SivaStreamingApp();
    JCommander cmd = new JCommander(cli, null, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    int errStatus = 0;
    try {
      cli.run();
    } catch (Exception ex) {
      LOG.error("Got error running app ", ex);
      errStatus = -1;
    } finally {
      System.exit(errStatus);
    }
  }

  /**
   *
   */
  public void run() throws Exception {
    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark Streaming APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[3]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    // spark.sparkContext().setLogLevel("INFO");

    // folder path clean up and creation, preparing the environment
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());
    fs.delete(new Path(streamingSourcePath), true);
    fs.delete(new Path(streamingCheckpointingPath), true);
    fs.delete(new Path(tablePath), true);
    fs.mkdirs(new Path(streamingSourcePath));

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    List<String> records1 = recordsToStrings(dataGen.generateInserts("001", 100));
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    String ckptPath = streamingCheckpointingPath + "/stream1";
    String srcPath = streamingSourcePath + "/stream1";
    if (!fs.exists(new Path(srcPath))) {
      fs.mkdirs(new Path(ckptPath));
      fs.mkdirs(new Path(srcPath));
    }

    // start streaming and showing
    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
        // setup the input for streaming
        Dataset<Row> streamingInput = spark.readStream().schema(inputDF1.schema()).json(srcPath + "/*");

        // thread for spark strucutured streaming
        Future<Void> streamFuture = executor.submit(() -> {
          LOG.info("===== Streaming Starting 111 =====");
          stream(streamingInput, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL(), ckptPath);
          LOG.info("===== Streaming Ends 111 =====");
          return null;
        });

        streamFuture.get();

      while (true) {
          LOG.warn("Sleeping for 5 secs");
          Thread.sleep(5000);
        HoodieTimeline timeline = HoodieDataSourceHelpers.allCompletedCommitsCompactions(fs, tablePath);
        System.out.println("Hoodie instants so far  :::::::::::::::: " + timeline.getInstants().collect(Collectors.toList()).size());
      }
    } finally {
      LOG.info("Shutting down executor 111 ======= ");
      executor.shutdownNow();
    }
  }

  /**
   * Hoodie spark streaming job.
   */
  public void stream(Dataset<Row> streamingInput, String operationType, String checkpointLocation) throws Exception {

    LOG.warn("Spark streaming to hudi table type " + tableType);

    DataStreamWriter<Row> writer = streamingInput.writeStream().format("org.apache.hudi")
        .option("hoodie.insert.shuffle.parallelism", "2").option("hoodie.upsert.shuffle.parallelism", "2")
        .option("hoodie.delete.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), operationType)
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP, "5")
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE_OPT_KEY(), "true")
        .option(HoodieWriteConfig.TABLE_NAME, tableName).option("checkpointLocation", checkpointLocation)
        .outputMode(OutputMode.Append());

    updateHiveSyncConfig(writer);
    StreamingQuery query = writer.trigger(Trigger.ProcessingTime(500)).start(tablePath);
    query.awaitTermination(streamingDurationInMs * 10000000);
  }

  /**
   * Setup configs for syncing to hive.
   */
  private DataStreamWriter<Row> updateHiveSyncConfig(DataStreamWriter<Row> writer) {
    if (enableHiveSync) {
      LOG.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), hiveTable)
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), hiveDB)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hiveJdbcUrl)
          .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hiveUser)
          .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hivePass)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (useMultiPartitionKeys) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day").option(
            DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
            MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dateStr");
      }
    }
    return writer;
  }
}
