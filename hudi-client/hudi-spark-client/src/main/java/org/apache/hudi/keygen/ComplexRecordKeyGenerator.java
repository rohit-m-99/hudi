/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.factory.SparkRecordKeyGeneratorInterface;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

public class ComplexRecordKeyGenerator implements SparkRecordKeyGeneratorInterface, RecordKeyGenerator {

  private final List<String> recordKeyFields;

  public ComplexRecordKeyGenerator(List<String> recordKeyFields) {
    this.recordKeyFields = recordKeyFields;
  }

  @Override
  public String getRecordKey(GenericRecord genericRecord) {
    return null;
  }

  @Override
  public String getRecordKey(Row row) {
    return null;
  }

  @Override
  public UTF8String getRecordKey(InternalRow row, StructType schema) {
    return null;
  }

}
