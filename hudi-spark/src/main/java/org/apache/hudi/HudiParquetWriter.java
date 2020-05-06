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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

/**
 * test docs.
 */
public class HudiParquetWriter implements MapPartitionsFunction<Row, Boolean> {

  String basePath;
  ExpressionEncoder<Row> encoder;
  SerializableConfiguration serConfig;

  public HudiParquetWriter(String basePath, ExpressionEncoder<Row> encoder, SerializableConfiguration serConfig) {
    this.basePath = basePath;
    this.encoder = encoder;
    this.serConfig = serConfig;
  }

  @Override
  public Iterator<Boolean> call(Iterator<Row> rowIterator) throws Exception {
    String fileId = UUID.randomUUID().toString();
    Path basePathDir = new Path(basePath);
    final FileSystem fs = FSUtils.getFs(basePath, serConfig.get());
    if (!fs.exists(basePathDir)) {
      fs.mkdirs(basePathDir);
    }
    System.out.println("Resolved path " + fs.resolvePath(basePathDir).toString());
    Path preFilePath = new Path(fs.resolvePath(basePathDir).toString() + "/" + fileId);
    System.out.println("File path chosen " + preFilePath.toString());

    List<Row> rows = new ArrayList<>();
    List<InternalRow> internalRows = new ArrayList<>();
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      rows.add(row);
      internalRows.add(encoder.toRow(row));
    }
    System.out.println("Total records collected " + rows.size());
    System.out.println("Total records :: " + Arrays.toString(rows.toArray()));
    System.out.println("Total internal rows :: " + Arrays.toString(internalRows.toArray()));
    try {
      System.out.println("resolved path " + preFilePath.toString());
      ParquetWriter<InternalRow> writer = new ParquetWriter<InternalRow>(preFilePath, new ParquetWriteSupport(), DEFAULT_COMPRESSION_CODEC_NAME, ParquetWriter.DEFAULT_BLOCK_SIZE,
          DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
          serConfig.get());

      int count = 0;
      for (InternalRow row : internalRows) {
        System.out.println("Writing Internal row " + (count++) + " :: " + row.toString());
        writer.write(row);
      }
      writer.close();
      return Collections.singleton(true).iterator();
    } catch (Exception e) {
      System.out.println("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
      e.printStackTrace();
      throw new IllegalStateException("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
    }
  }
}
