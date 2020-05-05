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
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieParquetRowWriter;
import org.apache.hudi.io.storage.HoodieRowParquetWriteSupport;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BulkInsertRowsMapFunction<T extends HoodieRecordPayload> implements MapPartitionsFunction<Row, InterimWriteStatus> {

  private static final Logger LOG = LogManager.getLogger(BulkInsertRowsMapFunction.class);

  private String instantTime;
  private HoodieWriteConfig config;
  private HoodieTable<T> hoodieTable;
  private ExpressionEncoder<Row> encoder;
  protected SparkTaskContextSupplier sparkTaskContextSupplier;
  private int filesWritten = 0;
  private boolean ignoreMetadataFields;

  public BulkInsertRowsMapFunction(String instantTime, HoodieWriteConfig config, HoodieTable<T> hoodieTable,
      ExpressionEncoder<Row> encoder) {
    this.instantTime = instantTime;
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.encoder = encoder;
    this.sparkTaskContextSupplier = hoodieTable.getSparkTaskContextSupplier();
    this.ignoreMetadataFields = config.ignoreMetadataFieldsBulkInsert();
  }

  @Override
  public Iterator<InterimWriteStatus> call(Iterator<Row> input) {
    List<InterimWriteStatus> writeStatuses = new ArrayList<>();
    // Create write token and filePrefix
    String writeToken = makeWriteToken();
    String filePrefix = FSUtils.createNewFileIdPfx();
    HoodieParquetRowWriter parquetRowWriter = null;

    try {
      while (input.hasNext()) {
        Row row = input.next();
        if (parquetRowWriter == null) { // first time
          parquetRowWriter = instantiateAndGetWriter(row, filePrefix, writeToken);
        }
        if (!parquetRowWriter.canWrite(row)) { // if reached max size, close and reopen another writer
          writeStatuses.add(parquetRowWriter.close());
          parquetRowWriter = instantiateAndGetWriter(row, filePrefix, writeToken);
        }
        parquetRowWriter.writeRow(row);
      }
      writeStatuses.add(parquetRowWriter.close());
    } catch (Throwable e) {
      LOG.error("Throwable(Global Error) thrown in BulkInsertRowsMapFunctions ", e);
      if (parquetRowWriter != null) {
        parquetRowWriter.setGlobalError(e);
        try {
          writeStatuses.add(parquetRowWriter.close());
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }
    return writeStatuses.iterator();
  }
  
  private HoodieParquetRowWriter instantiateAndGetWriter(Row row, String filePrefix, String writeToken) throws IOException {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(config.getBloomFilterNumEntries(), config.getBloomFilterFPP(),
            config.getDynamicBloomFilterMaxNumEntries(),
            config.getBloomFilterType());
    HoodieRowParquetWriteSupport writeSupport = new HoodieRowParquetWriteSupport(hoodieTable.getHadoopConf(), row.schema(), filter);
    return new HoodieParquetRowWriter(hoodieTable, config, row.getAs(config.getPartitionPathFieldProp()), getNextFileId(filePrefix),
        writeToken, instantTime, encoder, config.getParquetMaxFileSize(), config.getParquetCompressionRatio(),
        writeSupport, ignoreMetadataFields, ignoreMetadataFields ? -1 : row.fieldIndex(HoodieRecord.FILENAME_METADATA_FIELD),
        ignoreMetadataFields ? -1 : row.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        ignoreMetadataFields ? -1 : row.fieldIndex(HoodieRecord.PARTITION_PATH_METADATA_FIELD), ignoreMetadataFields ? -1 : row.fieldIndex(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        ignoreMetadataFields ? -1 : row.fieldIndex(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
  }

  private String getNextFileId(String filePrefix) {
    return filePrefix + "-" + (filesWritten++);
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
}