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

package org.apache.hudi.io;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.EncodableWriteStatus;
import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieRowParquetWriteSupport;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;

public class HoodieParquetRowWriter implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieParquetRowWriter.class);
  private static final long serialVersionUID = 1L;

  private static AtomicLong recordIndex = new AtomicLong(1);
  private FileSystem fs;
  private HoodieWrapperFileSystem wrapperFileSystem;
  private EncodableWriteStatus encodableWriteStatus;
  private HoodieWriteConfig config;
  private HoodieTable hoodieTable;
  private SparkTaskContextSupplier sparkTaskContextSupplier;
  private String instantTime;
  private ExpressionEncoder<Row> encoder;
  private Path hoodiePath;
  private Path path;
  private String fileId;
  private String partitionPath;
  private HoodieTimer timer;
  private HoodieRowParquetWriteSupport writeSupport;
  private ParquetWriter<InternalRow> writer;
  private long maxFileSize;
  private long recordsWritten = 0;
  private long insertRecordsWritten = 0;
  private int fileNameIndex;
  private int recordKeyIndex;
  private int partitionPathIndex;
  private int commitTimeIndex;
  private int commitSeqNoIndex;
  private String recordKeyProp;
  private String partitionPathProp;

  public HoodieParquetRowWriter(HoodieTable hoodieTable, HoodieWriteConfig config,
      String partitionPath, String fileId, String writeToken,
      String instantTime, ExpressionEncoder<Row> encoder, long maxFileSize, double compressionRatio,
      HoodieRowParquetWriteSupport writeSupport, int fileNameIndex, int recordKeyIndex,
      int partitionPathIndex, int commitTimeIndex, int commitSeqNoIndex) throws IOException {
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.fileId = fileId;
    this.partitionPath = partitionPath;
    this.sparkTaskContextSupplier = hoodieTable.getSparkTaskContextSupplier();
    this.fs = hoodieTable.getMetaClient().getFs();
    this.instantTime = instantTime;
    this.encoder = encoder;
    this.maxFileSize = maxFileSize + Math.round(maxFileSize * compressionRatio);
    this.fileNameIndex = fileNameIndex;
    this.recordKeyIndex = recordKeyIndex;
    this.partitionPathIndex = partitionPathIndex;
    this.commitSeqNoIndex = commitSeqNoIndex;
    this.commitTimeIndex = commitTimeIndex;
    this.recordKeyProp = config.getRecordKeyFieldProp();
    this.partitionPathProp = config.getPartitionPathFieldProp();
    this.writeSupport = writeSupport;
    this.timer = new HoodieTimer().startTimer();
    this.encodableWriteStatus = new EncodableWriteStatus(recordKeyProp);
    this.encodableWriteStatus.fileId = fileId;
    try {
      encodableWriteStatus.partitionPath = partitionPath;
      path = makeNewPath(partitionPath, writeToken, fileId, fs);
      Configuration localConf = registerFileSystem(path, hoodieTable.getHadoopConf());
      // convert to hoodiePath and instantiate WrapperFileSystem to assist in finding file size
      hoodiePath = HoodieWrapperFileSystem.convertToHoodiePath(path, localConf);
      wrapperFileSystem =
          (HoodieWrapperFileSystem) this.hoodiePath
              .getFileSystem(registerFileSystem(hoodiePath, localConf));

      // instantiate partition metadata and create marker file
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()),
          FSUtils.getPartitionPath(config.getBasePath(), partitionPath));
      partitionMetadata.trySave(getPartitionId());
      createMarkerFile(partitionPath, fs, writeToken, fileId);
      // instantiate writer
      writer = new ParquetWriter<InternalRow>(hoodiePath, writeSupport,
          config.getParquetCompressionCodec(), config.getParquetBlockSize(),
          config.getParquetPageSize(), (int) maxFileSize, DEFAULT_IS_DICTIONARY_ENABLED,
          DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
          writeSupport.getHadoopConf());
    } catch (Throwable e) {
      LOG.error("Throwable thrown during instantiation of HoodieCreateHandleRows ", e);
      encodableWriteStatus.globalError = e;
      throw e;
    }
  }

  public boolean canWrite(Row row) {
    return encodableWriteStatus.globalError == null
        && wrapperFileSystem.getBytesWritten(hoodiePath) < maxFileSize
        && row.getAs(config.getPartitionPathFieldProp()).equals(encodableWriteStatus.partitionPath);
  }

  public void writeRow(Row row) {
    try {
      String seqId =
          HoodieRecord.generateSequenceId(instantTime,
              sparkTaskContextSupplier.getPartitionIdSupplier().get(),
              recordIndex.getAndIncrement());
      Object[] rowFields = call(row);
      rowFields[fileNameIndex] = hoodiePath.getName();
      rowFields[recordKeyIndex] = row.getAs(recordKeyProp);
      rowFields[partitionPathIndex] = row.getAs(partitionPathProp);
      rowFields[commitTimeIndex] = instantTime;
      rowFields[commitSeqNoIndex] = seqId;
      Row rowWithMetadataFields = RowFactory.create(rowFields);

      InternalRow internalRow = encoder.toRow(rowWithMetadataFields);
      writer.write(internalRow);
      writeSupport.add(row.getAs(recordKeyProp));
      encodableWriteStatus.markSuccess(row);
      recordsWritten++;
      insertRecordsWritten++;
    } catch (Throwable e) {
      encodableWriteStatus.markFailure(row, row.getAs(recordKeyProp), e);
    }
  }

  public EncodableWriteStatus close() throws IOException {
    writer.close();
    encodableWriteStatus.path = path;
    encodableWriteStatus.recordsWritten = recordsWritten;
    encodableWriteStatus.insertRecordsWritten = insertRecordsWritten;
    encodableWriteStatus.endTime = timer.endTimer();

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(encodableWriteStatus.partitionPath);
    stat.setNumWrites(encodableWriteStatus.recordsWritten);
    stat.setNumDeletes(0);
    stat.setNumInserts(encodableWriteStatus.insertRecordsWritten);
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(encodableWriteStatus.fileId);
    if (path != null) {
      stat.setPath(new Path(config.getBasePath()), path);
      long fileSizeInBytes = FSUtils.getFileSize(fs, path);
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
    } else {
      stat.setTotalWriteBytes(0);
      stat.setFileSizeInBytes(0);
    }
    stat.setTotalWriteErrors(encodableWriteStatus.failedRows.size());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(encodableWriteStatus.endTime);
    stat.setRuntimeStats(runtimeStats);
    encodableWriteStatus.setStat(stat);
    return encodableWriteStatus;
  }

  public static Object[] call(Row row) throws Exception {
    Object[] result = new Object[row.size()];
    for (int i = 0; i < row.size(); i++) {
      result[i] = row.get(i);
    }
    return result;
  }

  public void setGlobalError(Throwable e) {
    if (encodableWriteStatus.globalError == null) {
      LOG.error("Setting Global error for " + partitionPath + ", fileId " + fileId + " :: " + e
          .getCause());
      encodableWriteStatus.globalError = e;
    } else {
      LOG.error(
          "Ignoring global error since its already set for " + partitionPath + ", fieId " + fileId
              + ". Existing " + encodableWriteStatus.globalError.getCause() + ".. New " + e
              .getCause());
    }
  }

  protected int getPartitionId() {
    return sparkTaskContextSupplier.getPartitionIdSupplier().get();
  }

  public static Configuration registerFileSystem(Path file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

  public Path makeNewPath(String partitionPath, String writeToken, String fileId, FileSystem fs) {
    Path path = FSUtils.getPartitionPath(config.getBasePath(), partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }

    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, fileId));
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  protected void createMarkerFile(String partitionPath, FileSystem fs, String writeToken,
      String fileId) {
    Path markerPath = makeNewMarkerPath(partitionPath, fs, writeToken, fileId);
    try {
      LOG.info("Creating Marker Path=" + markerPath);
      fs.create(markerPath, false).close();
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker file " + markerPath, e);
    }
  }

  /**
   * THe marker path will be <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename.
   */
  private Path makeNewMarkerPath(String partitionPath, FileSystem fs, String writeToken,
      String fileId) {
    Path markerRootPath = new Path(hoodieTable.getMetaClient().getMarkerFolderPath(instantTime));
    Path path = FSUtils.getPartitionPath(markerRootPath, partitionPath);
    try {
      fs.mkdirs(path); // create a new partition as needed.
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    return new Path(path.toString(), FSUtils.makeMarkerFile(instantTime, writeToken, fileId));
  }
}
