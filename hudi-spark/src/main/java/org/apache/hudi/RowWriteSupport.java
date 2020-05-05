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
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Row;

import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

// implementation of WriteSupport for Row's used by the native ParquetWriter. Not fully completed. 
// https://www.programcreek.com/scala/org.apache.parquet.hadoop.api.WriteSupport
public class RowWriteSupport extends WriteSupport<Row> {

  private RowWriter writer;
  MessageType schema;
  RoundingMode roundingMode;
  Map<String, String> metadata;

  public RowWriteSupport(MessageType schema,
                         RoundingMode roundingMode,
                         Map<String, String> metadata) {
    this.schema = schema;
    this.roundingMode = roundingMode;
    this.metadata = metadata;
  }

  public FinalizedWriteContext finalizeWrite() {
    return new FinalizedWriteContext(metadata);
  }

  public WriteSupport.WriteContext init(Configuration configuration) {
    return new WriteSupport.WriteContext(schema, new HashMap<>());
  }

  public void prepareForWrite(RecordConsumer record) {
    writer = new RowWriter(record, roundingMode);
  }

  public void write(Row row) {
    writer.write(row);
  }

  class RowWriter {
    RecordConsumer record;
    RoundingMode roundingMode;

    RowWriter(RecordConsumer record, RoundingMode roundingMode) {
      this.record = record;
      this.roundingMode = roundingMode;
    }

    void write(Row row) {
      record.startMessage();
      // TODO
      /*StructRecordWriter writer = new StructRecordWriter(row.schema, roundingMode, false);
      writer.write(record, row.getValuesMap());
      */
      record.endMessage();
    }
  }

}



/*
class RowWriter(record: RecordConsumer, roundingMode: RoundingMode) {

    def write(row: Row): Unit = {
    record.startMessage()
    val writer = new StructRecordWriter(row.schema, roundingMode, false)
    writer.write(record, row.values)
    record.endMessage()
    }
    }*/