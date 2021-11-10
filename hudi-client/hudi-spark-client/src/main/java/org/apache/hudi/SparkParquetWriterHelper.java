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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class SparkParquetWriterHelper {
  private static final Logger LOG = LoggerFactory.getLogger(SparkParquetWriterHelper.class);

  private final String basePath;
  private int numCols;
  private int numRecordsPerFile;
  private int numFilesPerWriter;
  private boolean autoGenerateSchema;
  private String schemaFilePath;

  public SparkParquetWriterHelper(String basePath, int numCols, int numRecordsPerFile) {
    this(basePath, numCols, numRecordsPerFile, 1, true, null);
  }

  public SparkParquetWriterHelper(String basePath, int numCols, int numRecordsPerFile, int numFilesPerWriter, boolean autoGenerateSchema,
                                  String schemaFilePath) {
    this.basePath = basePath;
    this.numCols = numCols;
    this.numRecordsPerFile = numRecordsPerFile;
    this.numFilesPerWriter = numFilesPerWriter;
    this.autoGenerateSchema = autoGenerateSchema;
    this.schemaFilePath = schemaFilePath;
  }

  public void generateRecordsToFile() throws IOException {
    int counter = 0;
    while (counter++ < numFilesPerWriter) {
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(basePath, fileId);
      Configuration conf = new Configuration();
      AvroRecGenerator recordGenerator = new AvroRecGenerator(numCols, numRecordsPerFile, autoGenerateSchema, schemaFilePath);
      ParquetWriter parquetWriter = new AvroParquetWriter(filePath, recordGenerator.getSchema(),
          CompressionCodecName.GZIP, 100 * 1024 * 1024, 1024 * 1024, true, conf);
      while (recordGenerator.hasNext()) {
        parquetWriter.write(recordGenerator.next());
      }
      parquetWriter.close();
    }
  }
}
