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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieDSRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRowRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import scala.Function1;

public class ConversionUtils {

  static Dataset<HoodieRowRecord> createDataset(Dataset<Row> df, Schema avroSchema, String structName, String recordNamespace) {
    // Use the Avro schema to derive the StructType which has the correct nullability information
    StructType dataType = (StructType) SchemaConverters.toSqlType(avroSchema).dataType();

    return df.mapPartitions((MapPartitionsFunction<Row, HoodieRowRecord>) input -> {
      Function1<Object, Object> converter = AvroConversionHelper.createConverterToAvro(dataType, structName, recordNamespace);
      List<HoodieRowRecord> rowRecords = new ArrayList<>();
      int count = 0;
      while (input.hasNext()) {
        HoodieRowRecord rowRecord = new HoodieRowRecord(HoodieAvroUtils.avroToBytes((GenericRecord) converter.apply(input.next())));
        rowRecord.setRecordKey("key" + (count));
        rowRecord.setPartitionPath("p_p" + (count % 5));
        rowRecord.setOrderingVal(count);
        count++;
        rowRecords.add(rowRecord);
      }
      return rowRecords.iterator();
    }, Encoders.bean(HoodieRowRecord.class));
  }

  static Dataset<HoodieDSRecord> createHoodieDSRecordDataset(Dataset<Row> df, Schema avroSchema, String structName, String recordNamespace) {
    // Use the Avro schema to derive the StructType which has the correct nullability information
    StructType dataType = (StructType) SchemaConverters.toSqlType(avroSchema).dataType();

    return df.mapPartitions((MapPartitionsFunction<Row, HoodieDSRecord>) input -> {
      Function1<Object, Object> converter = AvroConversionHelper.createConverterToAvro(dataType, structName, recordNamespace);
      List<HoodieDSRecord> rowRecords = new ArrayList<>();
      int count = 0;
      while (input.hasNext()) {
        HoodieDSRecord rowRecord = new HoodieDSRecord<>(HoodieAvroUtils.avroToBytes((GenericRecord) converter.apply(input.next())));
        rowRecord.setKey(new HoodieKey("key" + (count), "p_p" + (count % 5)));
        rowRecord.setOrderingVal(count);
        count++;
        rowRecords.add(rowRecord);
      }
      return rowRecords.iterator();
    }, Encoders.bean(HoodieDSRecord.class));
  }

  static Dataset<HoodieRecordPayload> createDatasetHoodieRecords(Dataset<Row> df, Schema avroSchema, String structName, String recordNamespace) {
    // Use the Avro schema to derive the StructType which has the correct nullability information
    StructType dataType = (StructType) SchemaConverters.toSqlType(avroSchema).dataType();

    return df.mapPartitions((MapPartitionsFunction<Row, HoodieRecordPayload>) input -> {
      Function1<Object, Object> converter = AvroConversionHelper.createConverterToAvro(dataType, structName, recordNamespace);
      List<HoodieRecordPayload> hoodieRecords = new ArrayList<>();
      int count = 0;
      while (input.hasNext()) {
        //count++;
        GenericRecord genericRecord = (GenericRecord) converter.apply(input.next());
        hoodieRecords.add(new OverwriteWithLatestAvroPayload(Option.of(genericRecord)));
      }
      return hoodieRecords.iterator();
    }, Encoders.bean(HoodieRecordPayload.class));
  }

  /*private ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }*/

}