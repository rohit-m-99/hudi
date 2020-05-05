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

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieDataSourceHelpers;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.utils.DataSourceTestUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Sample program that writes & reads hoodie tables via the Spark datasource.
 */
public class HoodieJavaApp {

  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
  private String tablePath = "file:///tmp/hoodie/sample-table";

  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
  private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
  private String tableType = HoodieTableType.COPY_ON_WRITE.name();

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

  @Parameter(names = {"--non-partitioned", "-np"}, description = "Use non-partitioned Table")
  private Boolean nonPartitionedTable = false;

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
  private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  private static final Logger LOG = LogManager.getLogger(HoodieJavaApp.class);

  public static void main(String[] args) throws Exception {
    HoodieJavaApp cli = new HoodieJavaApp();
    JCommander cmd = new JCommander(cli, null, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }
    cli.run();
  }

  public void run() throws Exception {

    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").master("local[1]").getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    spark.sparkContext().setLogLevel("WARN");
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());

    // Generator of some records to be loaded in.
    HoodieTestDataGenerator dataGen = null;
    if (nonPartitionedTable) {
      // All data goes to base-path
      dataGen = new HoodieTestDataGenerator(new String[] {""});
    } else {
      dataGen = new HoodieTestDataGenerator();
    }

    /**
     * Commit with only inserts
     */
    // Generate some input..
    List<HoodieRecord> recordsSoFar = new ArrayList<>(dataGen.generateInserts("001"/* ignore */, 100));
    List<String> records1 = DataSourceTestUtils.convertToStringList(recordsSoFar);
    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    // Save as hoodie dataset (copy on write)
    // specify the hoodie source
    DataFrameWriter<Row> writer = inputDF1.write().format("org.apache.hudi")
        // any hoodie client config can be passed like this
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.bulkinsert.shuffle.parallelism", "2")
        .option("hoodie.bulkinsert.compression.codec", "SNAPPY")
        // full list in HoodieWriteConfig & its package
        .option("hoodie.upsert.shuffle.parallelism", "2")
        // Hoodie Table Type
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        // insert
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.BULK_INSERT_ROWS_OPERATION_OPT_VAL())
        // This is the record key
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        // this is the partition to place it into
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        // use to combine duplicate records in input/with disk val
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        // Used by hive sync and queries
        .option(HoodieWriteConfig.TABLE_NAME, tableName)
        // Add Key Extractor
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName()
                : SimpleKeyGenerator.class.getCanonicalName())
        // This will remove any existing data at path below, and create a
        .mode(SaveMode.Overwrite);

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("First commit at instant time :" + commitInstantTime1);

    /**
     * Commit that updates records
     */
    /*List<HoodieRecord> recordsToBeUpdated = dataGen.generateUpdates("002", 100);
    recordsSoFar.addAll(recordsToBeUpdated);
    List<String> records2 = org.apache.hudi.utils.DataSourceTestUtils.convertToStringList(recordsToBeUpdated);
    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));
    writer = inputDF2.write().format("org.apache.hudi").option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "timestamp")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName()
                : SimpleKeyGenerator.class.getCanonicalName()) // Add Key Extractor
        .option(HoodieWriteConfig.TABLE_NAME, tableName).mode(SaveMode.Append);

    updateHiveSyncConfig(writer);
    writer.save(tablePath);
    String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("Second commit at instant time :" + commitInstantTime2);

    /**
     * Commit that Deletes some records
     */
    /*List<String> deletes = org.apache.hudi.utils.DataSourceTestUtils.convertKeysToStringList(
        HoodieClientTestUtils
            .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(recordsSoFar), 20));
    Dataset<Row> inputDF3 = spark.read().json(jssc.parallelize(deletes, 2));
    writer = inputDF3.write().format("org.apache.hudi").option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType) // Hoodie Table Type
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), "delete")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "partition")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "_row_key")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(),
            nonPartitionedTable ? NonpartitionedKeyGenerator.class.getCanonicalName()
                : SimpleKeyGenerator.class.getCanonicalName()) // Add Key Extractor
        .option(HoodieWriteConfig.TABLE_NAME, tableName).mode(SaveMode.Append);

    updateHiveSyncConfig(writer);
    writer.save(tablePath);
    String commitInstantTime3 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    LOG.info("Third commit at instant time :" + commitInstantTime3);

    /**
     * Read & do some queries
     */
    Dataset<Row> snapshotQueryDF = spark.read().format("org.apache.hudi")
        // pass any path glob, can include hoodie & non-hoodie
        // datasets
        .load(tablePath + (nonPartitionedTable ? "/*" : "/*/*/*/*"));
    snapshotQueryDF.registerTempTable("hoodie_ro");
    spark.sql("describe hoodie_ro").show();
    // all trips whose fare amount was greater than 2.
    spark.sql("select fare.amount, begin_lon, begin_lat, timestamp from hoodie_ro where fare.amount > 2.0").show();

    /*if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) {
      /**
       * Consume incrementally, only changes in commit 2 above. Currently only supported for COPY_ON_WRITE TABLE
       */
    /*Dataset<Row> incQueryDF = spark.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
         // Only changes in write 2 above
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), commitInstantTime1)
          // For incremental view, pass in the root/base path of dataset
          .load(tablePath);

      LOG.info("You will only see records from : " + commitInstantTime2);
      incQueryDF.groupBy(incQueryDF.col("_hoodie_commit_time")).count().show();
    }*/
  }

  /**
   * Setup configs for syncing to hive.
   */
  private DataFrameWriter<Row> updateHiveSyncConfig(DataFrameWriter<Row> writer) {
    if (enableHiveSync) {
      LOG.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), hiveTable)
          .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), hiveDB)
          .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hiveJdbcUrl)
          .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hiveUser)
          .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hivePass)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (nonPartitionedTable) {
        writer = writer
            .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
                NonPartitionedExtractor.class.getCanonicalName())
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "");
      } else if (useMultiPartitionKeys) {
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
