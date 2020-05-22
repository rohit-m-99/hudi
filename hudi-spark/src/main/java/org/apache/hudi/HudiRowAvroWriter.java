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
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

public class HudiRowAvroWriter implements MapPartitionsFunction<Row, Boolean> {

  String basePath;
  SerializableConfiguration serConfig;
  String compressionCodec;

  public HudiRowAvroWriter(String basePath, SerializableConfiguration serConfig, String compressionCodec) {
    this.basePath = basePath;
    this.serConfig = serConfig;
    this.compressionCodec = compressionCodec;
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
        int count = 1;
        Row firstRow = rowIterator.next();

        ParquetWriter<Row> parquetWriter =
            AvroParquetWriter.<Row>builder(preFilePath).withCompressionCodec(CompressionCodecName.fromConf(compressionCodec)).build();

        parquetWriter.write(firstRow);
        while (rowIterator.hasNext()) {
          parquetWriter.write(rowIterator.next());
          count++;
        }
        parquetWriter.close();
        System.out.println("Total rows for this partition " + count);
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