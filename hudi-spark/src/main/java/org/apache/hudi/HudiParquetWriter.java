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

/**
 * test docs.
 */
public class HudiParquetWriter implements MapPartitionsFunction<Row, Boolean> {

  String basePath;
  ExpressionEncoder<Row> encoder;

  public HudiParquetWriter(String basePath, ExpressionEncoder<Row> encoder) {
    this.basePath = basePath;
    this.encoder = encoder;
  }

  @Override
  public Iterator<Boolean> call(Iterator<Row> rowIterator) throws Exception {
    String fileId = UUID.randomUUID().toString();
    Path preFilePath = new Path(basePath + "/" + fileId);
    System.out.println("File path chosen " + preFilePath.toString());
    List<Row> rows = new ArrayList<>();
    while (rowIterator.hasNext()) {
      rows.add(rowIterator.next());
    }
    System.out.println("Total records collected " + rows.size());
    System.out.println("Total records :: " + Arrays.toString(rows.toArray()));

    try {
      Configuration config = new Configuration();
      preFilePath.getFileSystem(config).mkdirs(preFilePath);
      preFilePath = preFilePath.getFileSystem(config).resolvePath(preFilePath);
      System.out.println("resolved path " + preFilePath.toString());
      ParquetWriter<InternalRow> writer = new ParquetWriter<>(preFilePath, new ParquetWriteSupport());
      System.out.println("Instantiated writer successfully ");
      int count = 0;
      for (Row row : rows) {
        System.out.println("Writing row " + (count) + " :: " + row.mkString());
        InternalRow internalRow = encoder.toRow(row);
        System.out.println("Writing Internal row " + (count++) + " :: " + internalRow.toString());
        writer.write(internalRow);
      }
      writer.close();
      return Collections.singleton(true).iterator();
    } catch (Exception e) {
      System.out.println("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
      throw new IllegalStateException("Exception thrown while instantiating or writing to Parquet " + e.getMessage() + " ... cause " + e.getCause());
    }
  }
}
