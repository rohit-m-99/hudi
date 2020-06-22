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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.InterimWriteStatus;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class BulkInsertHelperRows {

  private static final Logger LOG = LogManager.getLogger(BulkInsertHelperRows.class);

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata bulkInsertRows(JavaSparkContext jsc,
      Dataset<Row> inputRecords, String instantTime,
      HoodieTable<T> table, HoodieWriteConfig config,
      CommitActionExecutor<T> executor, boolean performDedupe,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    // De-dupe/merge if needed
    Dataset<Row> dedupedRecords = inputRecords;

    /*if (performDedupe) {
      dedupedRecords = WriteHelper.combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          config.getInsertShuffleParallelism(), ((HoodieTable<T>)table));
    }*/

    // no user defined repartitioning support yet

    final Dataset<Row> rows = dedupedRecords.sort(config.getPartitionPathFieldProp(), config.getRecordKeyFieldProp()).coalesce(config.getBulkInsertShuffleParallelism());

    Dataset<Row> repartitionedRecords = rows;
    if (!config.ignoreMetadataFieldsBulkInsert()) {
      repartitionedRecords = rows
          .withColumn(HoodieRecord.FILENAME_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType))
          .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType))
          .withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType))
          .withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType))
          .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType));
    }

    /* // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());*/

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
        table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    // Generate encoder for Row
    ExpressionEncoder encoder = getEncoder(repartitionedRecords.schema());

    try {
      Dataset<InterimWriteStatus> interimWriteStatusDataset = repartitionedRecords.mapPartitions(
          new BulkInsertRowsMapFunction<>(instantTime, config, table, encoder), Encoders.bean(InterimWriteStatus.class));

      // JavaRDD<WriteStatus> writeStatusRDD = getWriteStatusFromInterimWriteStatus(jsc, interimWriteStatusDataset, table, config);

      /*try{
      Dataset<WriteStatus> interimWriteStatusDataset = repartitionedRecords.mapPartitions(
        new BulkInsertRowsMapFunction<>(instantTime, config, table, encoder), Encoders.bean(WriteStatus.class));

      JavaRDD<WriteStatus> writeStatusRDD = interimWriteStatusDataset.toJavaRDD();
      */
      /*List<WriteStatus> writeStatusList = writeStatusRDD.collect();
      for (WriteStatus writeStatus : writeStatusList) {
        System.out.println("Write status " + writeStatus);
        LOG.info("Write status : " + writeStatus);
      }*/

      /*List<InterimWriteStatus> interimWriteStatuses = interimWriteStatusDataset.collectAsList();
      System.out.println("Total interim write status " + interimWriteStatuses.size());
      for (InterimWriteStatus interimWriteStatus : interimWriteStatuses) {
        System.out.println("Interim " + interimWriteStatus + " :: Write stat " + interimWriteStatus.getStat());
      }*/

      executor.updateIndexAndCommitIfNeeded(interimWriteStatusDataset, result);
      return result;
    } catch (Throwable e) {
      LOG.error("Throwable thrwon in map partition func ", e);
      throw e;
    }
  }

  private static ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(), SimpleAnalyzer$.MODULE$);
  }

  /*private static JavaRDD<WriteStatus> getWriteStatusFromInterimWriteStatus(JavaSparkContext jsc, Dataset<InterimWriteStatus> interimWriteStatusDataset, HoodieTable table, HoodieWriteConfig config) {

    return interimWriteStatusDataset.toJavaRDD().map(entry -> {
      WriteStatus writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
          !table.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
      writeStatus.setFileId(entry.fileId);
      writeStatus.setPartitionPath(entry.partitionPath);
      for (Row row : entry.successRows) {
        writeStatus.markSuccess(row);
      }
      for (Tuple3<Row, String, Throwable> erroredRow : entry.failedRows) {
        writeStatus.markFailure(erroredRow._1(), erroredRow._2(), erroredRow._3());
      }
      if (entry.globalError != null) {
        writeStatus.setGlobalError(entry.globalError);
      }
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(writeStatus.getPartitionPath());
      stat.setNumWrites(entry.recordsWritten);
      stat.setNumDeletes(0);
      stat.setNumInserts(entry.insertRecordsWritten);
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(writeStatus.getFileId());
      if (entry.path != null) {
        stat.setPath(new Path(config.getBasePath()), entry.path);
        long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), entry.path);
        stat.setTotalWriteBytes(fileSizeInBytes);
        stat.setFileSizeInBytes(fileSizeInBytes);
      } else {
        stat.setTotalWriteBytes(0);
        stat.setFileSizeInBytes(0);
      }
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalCreateTime(entry.endTime);
      stat.setRuntimeStats(runtimeStats);
      writeStatus.setStat(stat);
      return writeStatus;
    });
  } */

  private static JavaRDD<WriteStatus> getWriteStatusFromInterimWriteStatus(JavaSparkContext jsc, Dataset<InterimWriteStatus> interimWriteStatusDataset, HoodieTable table, HoodieWriteConfig config) {

    return interimWriteStatusDataset.toJavaRDD().map(entry -> {
      WriteStatus writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
          !table.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
      /*writeStatus.setFileId(entry.getFileId());
      writeStatus.setPartitionPath(entry.getPartitionPath());
      for (Row row : entry.getSuccessRows()) {
        writeStatus.markSuccess(row);
      }
      for (Tuple3<Row, String, Throwable> erroredRow : entry.getFailedRows()) {
        writeStatus.markFailure(erroredRow._1(), erroredRow._2(), erroredRow._3());
      }
      if (entry.getGlobalError() != null) {
        writeStatus.setGlobalError(entry.getGlobalError());
      }
      writeStatus.setStat(entry.getStat());*/

      writeStatus.setFileId(entry.fileId);
      writeStatus.setPartitionPath(entry.partitionPath);
      for (Row row : entry.successRows) {
        writeStatus.markSuccess(row);
      }
      for (Tuple3<Row, String, Throwable> erroredRow : entry.failedRows) {
        writeStatus.markFailure(erroredRow._1(), erroredRow._2(), erroredRow._3());
      }
      if (entry.globalError != null) {
        writeStatus.setGlobalError(entry.globalError);
      }

      /*HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(writeStatus.getPartitionPath());
      stat.setNumWrites(entry.recordsWritten);
      stat.setNumDeletes(0);
      stat.setNumInserts(entry.insertRecordsWritten);
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(writeStatus.getFileId());
      if (entry.path != null) {
        stat.setPath(new Path(config.getBasePath()), entry.path);
        long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), entry.path);
        stat.setTotalWriteBytes(fileSizeInBytes);
        stat.setFileSizeInBytes(fileSizeInBytes);
      } else {
        stat.setTotalWriteBytes(0);
        stat.setFileSizeInBytes(0);
      }
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalCreateTime(entry.endTime);
      stat.setRuntimeStats(runtimeStats);
      writeStatus.setStat(stat);*/

      writeStatus.setStat(entry.getStat());

      /*HoodieWriteStat stat = new HoodieWriteStat();
      stat.setPartitionPath(writeStatus.getPartitionPath());
      stat.setNumWrites(entry.getRecordsWritten());
      stat.setNumDeletes(0);
      stat.setNumInserts(entry.getInsertRecordsWritten());
      stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      stat.setFileId(writeStatus.getFileId());
      if (entry.getPath() != null) {
        stat.setPath(new Path(config.getBasePath()), entry.getPath());
        long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), entry.getPath());
        stat.setTotalWriteBytes(fileSizeInBytes);
        stat.setFileSizeInBytes(fileSizeInBytes);
      } else {
        stat.setTotalWriteBytes(0);
        stat.setFileSizeInBytes(0);
      }
      stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
      HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
      runtimeStats.setTotalCreateTime(entry.getEndTime());
      stat.setRuntimeStats(runtimeStats);
      writeStatus.setStat(stat); */
      return writeStatus;
    });
  }

}