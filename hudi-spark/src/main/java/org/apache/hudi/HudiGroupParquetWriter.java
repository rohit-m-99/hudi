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

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

public class HudiGroupParquetWriter implements MapPartitionsFunction<Row, Boolean> {

  String basePath;
  ExpressionEncoder<Row> encoder;
  SerializableConfiguration serConfig;
  String compressionCodec;
  Schema schema;

  private static final Logger LOG = LoggerFactory.getLogger(HudiRowParquetWriter.class);

  public HudiGroupParquetWriter(String basePath, ExpressionEncoder<Row> encoder, SerializableConfiguration serConfig,
                                String compressionCodec, Schema schema) {
    this.basePath = basePath;
    this.encoder = encoder;
    this.serConfig = serConfig;
    this.compressionCodec = compressionCodec;
    this.schema = schema;
  }

  @Override
  public Iterator<Boolean> call(Iterator<Row> rowIterator) throws Exception {
    if (rowIterator.hasNext()) {
      try {
        String fileId = UUID.randomUUID().toString();
        Path basePathDir = new Path(basePath);
        final FileSystem fs = FSUtils.getFs(basePath, serConfig.get());
        if (!fs.exists(basePathDir)) {
          fs.mkdirs(basePathDir);
        }
        Path preFilePath = new Path(fs.resolvePath(basePathDir).toString() + "/" + fileId);
        int count = 0;
        if (rowIterator.hasNext()) {
          Row firstRow = rowIterator.next();
          List<Row> rowsWritten = new ArrayList<>();
          Configuration config = serConfig.get();
          config.set("spark.sql.parquet.writeLegacyFormat", "false");
          config.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS");

          MessageType parquetSchema = new AvroSchemaConverter().convert(schema);
          GroupWriteSupportLocal.setSchema(parquetSchema, serConfig.get());
          GroupWriteSupportLocal writeSupport = new GroupWriteSupportLocal();

          ParquetWriter<Group> writer = new GroupParquetWriter(preFilePath, writeSupport, CompressionCodecName.fromConf(compressionCodec), ParquetWriter.DEFAULT_BLOCK_SIZE,
              DEFAULT_PAGE_SIZE, DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
              config);

          rowsWritten.add(firstRow);
          InternalRow internalRow = encoder.toRow(firstRow);
          // writer.write(internalRow);
          count++;
          while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            internalRow = encoder.toRow(row);
            // writer.write(internalRow);
            rowsWritten.add(row);
            count++;
          }
          writer.close();
        }
        System.out.println("Total rows for this partition " + count);
        LOG.info("Write complete :::::::::: ");

        // reading.
        /*ParquetReadSupport readSupport = new ParquetReadSupport();
        config.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), firstRow.schema().json());
        config.set("spark.sql.parquet.binaryAsString", "true");
        config.set("spark.sql.parquet.int96AsTimestamp","false");
        ParquetReader<InternalRow> reader = new ParquetReader(config, preFilePath, readSupport);
        InternalRow internalRowRead = reader.read();
        List<InternalRow> internalRowsRead = new ArrayList<>();
        internalRowsRead.add(internalRowRead);

        List<Row> readRows = new ArrayList<>();
        readRows.add(encoder.fromRow(internalRowRead));
        int counter = 0;
        int found = 0;
        while(internalRowRead != null) {
          internalRowsRead.add(internalRowRead);
          Row row = encoder.fromRow(internalRowRead);
          readRows.add(row);
          if(!rowsWritten.contains(row)){
            System.out.println("Row not found "+ row);
            counter++;
          } else{
            found++;
          }
          internalRowRead = reader.read();
        }
        reader.close();
        System.out.println("Total found " + found +". total misss " + counter +" total written : "+ internalRowsWritten.size() +" total read size "+ internalRowsRead.size());
        LOG.info("Read complete :::::::::: "); */
        return Collections.singleton(true).iterator();
      } catch (Exception e) {
        System.out.println("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
        e.printStackTrace();
        throw new IllegalStateException("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
      }
    } else {
      return Collections.singleton(true).iterator();
    }
  }
}
