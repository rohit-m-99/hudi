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

import org.apache.hudi.common.util.FileIOUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AvroRecGenerator implements Iterator<GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroRecGenerator.class);

  private Schema schema = null;
  private final long totalRecords;
  private long curIndex = 0;
  private GenericRecordPayloadGenerator generator;

  public AvroRecGenerator(int numCols, long totalRecords, boolean autoGenerateSchema, String schemaFilePath) throws IOException {
    this.totalRecords = totalRecords;
    if (autoGenerateSchema) {
      String stringIdPrefix = "col_str_";
      String longIdPrefix = "col_long_";
      String intIdPrefix = "col_int_";
      String doubleIdPrefix = "col_double_";
      String booleanIdPrefix = "col_bool_";

      List<Schema.Field> schemaFields = new ArrayList<>();
      for (int i = 1; i < numCols; i++) {
        if (i % 5 == 0) {
          schemaFields.add(new Schema.Field(stringIdPrefix + String.format("%04d", i), Schema.create(Schema.Type.STRING), "", null));
        } else if (i % 5 == 1) {
          schemaFields.add(new Schema.Field(longIdPrefix + String.format("%04d", i), Schema.create(Schema.Type.LONG), "", null));
        } else if (i % 5 == 2) {
          schemaFields.add(new Schema.Field(intIdPrefix + String.format("%04d", i), Schema.create(Schema.Type.INT), "", null));
        } else if (i % 5 == 3) {
          schemaFields.add(new Schema.Field(doubleIdPrefix + String.format("%04d", i), Schema.create(Schema.Type.DOUBLE), "", null));
        } else {
          schemaFields.add(new Schema.Field(booleanIdPrefix + String.format("%04d", i), Schema.create(Schema.Type.BOOLEAN), "", null));
        }
      }
      schema = Schema.createRecord("test_schema", "sample_doc", "sample_namespace", false, schemaFields);
    } else {
      schema = new Schema.Parser().parse(FileIOUtils.readAsUTFString(new FileInputStream(schemaFilePath)));
      LOG.warn(schema.toString());
    }
    generator = new GenericRecordPayloadGenerator(schema);
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean hasNext() {
    return curIndex < totalRecords;
  }

  @Override
  public GenericRecord next() {
    GenericRecord genericRecord = generator.getNewPayload();
    curIndex++;
    return genericRecord;
  }
}
