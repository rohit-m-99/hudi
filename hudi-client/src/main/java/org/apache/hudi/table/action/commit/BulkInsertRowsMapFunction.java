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

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

public class BulkInsertRowsMapFunction<T extends HoodieRecordPayload> implements MapPartitionsFunction<Row, WriteStatus> {

  private String instantTime;
  private HoodieWriteConfig config;
  private HoodieTable<T> hoodieTable;
  private List<String> fileIDPrefixes;
  private ExpressionEncoder<Row> encoder;
  private long recordsWritten = 0;
  private long insertRecordsWritten = 0;
  protected SparkTaskContextSupplier sparkTaskContextSupplier;
  protected final FileSystem fs;

  public BulkInsertRowsMapFunction(String instantTime, HoodieWriteConfig config, HoodieTable<T> hoodieTable,
                                   List<String> fileIDPrefixes, ExpressionEncoder<Row> encoder) {
    this.instantTime = instantTime;
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.fileIDPrefixes = fileIDPrefixes;
    this.encoder = encoder;
    this.sparkTaskContextSupplier = hoodieTable.getSparkTaskContextSupplier();
    this.fs = getFileSystem();
  }

  @Override
  public Iterator<WriteStatus> call(Iterator<Row> input) throws Exception {
    HoodieTimer timer = new HoodieTimer().startTimer();
    WriteStatus writeStatus = (WriteStatus) ReflectionUtils.loadClass(config.getWriteStatusClassName(),
        !hoodieTable.getIndex().isImplicitWithStorage(), config.getWriteStatusFailureFraction());
    Path preFilePath = null;
    if (input.hasNext()) {
      try {
        String writeToken = makeWriteToken();
        String filePrefix = FSUtils.createNewFileIdPfx();
        preFilePath = makeNewPath(config.getPartitionPathFieldProp(), writeToken, filePrefix);
        writeStatus.setFileId(filePrefix);
        Row firstRow = input.next();
        String partitionPath = firstRow.getAs(config.getPartitionPathFieldProp());
        writeStatus.setPartitionPath(partitionPath);

        List<Row> rowsWritten = new ArrayList<>();
        Configuration hadoopConf = hoodieTable.getHadoopConf();
        hadoopConf.set("spark.sql.parquet.writeLegacyFormat", "false");
        hadoopConf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS");
        ParquetWriteSupport writeSupport = new ParquetWriteSupport();
        writeSupport.setSchema(firstRow.schema(), hadoopConf);
        ParquetWriter<InternalRow> writer = new ParquetWriter<InternalRow>(preFilePath, writeSupport, CompressionCodecName.fromConf("SNAPPY"), ParquetWriter.DEFAULT_BLOCK_SIZE,
            DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
            hadoopConf);
        rowsWritten.add(firstRow);

        int count = 0;
        try {
          InternalRow internalRow = encoder.toRow(firstRow);
          writer.write(internalRow);
          writeStatus.markSuccess(firstRow);
          recordsWritten = 1;
          insertRecordsWritten = 1;
          count = 1;
        } catch (Throwable e) {
          System.out.println("Throwable thrown while writing first record " + firstRow);
          writeStatus.markFailure(firstRow, firstRow.getAs(config.getRecordKeyFieldProp()), e);
        }
        while (input.hasNext()) {
          Row row = input.next();
          try {
            InternalRow internalRow = encoder.toRow(row);
            writer.write(internalRow);
            rowsWritten.add(row);
            count++;
            recordsWritten++;
            insertRecordsWritten++;
            writeStatus.markSuccess(row);
          } catch (Throwable e) {
            System.out.println("Throwable thrown while writing record " + row);
            writeStatus.markFailure(row, row.getAs(config.getRecordKeyFieldProp()), e);
          }
        }
        writer.close();
      } catch (Throwable e) {
        System.out.println("Global Throwable thrown while writing records ");
        writeStatus.setGlobalError(e);
      }
      updateWriteStatus(writeStatus, preFilePath, timer);
    }
    return Collections.singleton(writeStatus).iterator();
  }

  protected FileSystem getFileSystem() {
    return hoodieTable.getMetaClient().getFs();
  }

  protected int getPartitionId() {
    return sparkTaskContextSupplier.getPartitionIdSupplier().get();
  }

  protected int getStageId() {
    return sparkTaskContextSupplier.getStageIdSupplier().get();
  }

  protected long getAttemptId() {
    return sparkTaskContextSupplier.getAttemptIdSupplier().get();
  }

  /**
   * Generate a write token based on the currently running spark task and its place in the spark dag.
   */
  private String makeWriteToken() {
    return FSUtils.makeWriteToken(getPartitionId(), getStageId(), getAttemptId());
  }

  public Path makeNewPath(String partitionPath, String writeToken, String fileId) {
    Path path = FSUtils.getPartitionPath(config.getBasePath(), partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, fileId));
  }

  private void updateWriteStatus(WriteStatus writeStatus, Path path, HoodieTimer timer) throws IOException {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setNumWrites(recordsWritten);
    stat.setNumDeletes(0);
    stat.setNumInserts(insertRecordsWritten);
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(writeStatus.getFileId());
    if (path != null) {
      stat.setPath(new Path(config.getBasePath()), path);
      long fileSizeInBytes = FSUtils.getFileSize(fs, path);
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
    } else {
      stat.setTotalWriteBytes(0);
      stat.setFileSizeInBytes(0);
    }
    stat.setTotalWriteErrors(writeStatus.getTotalErrorRecords());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(timer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    writeStatus.setStat(stat);
  }
}
