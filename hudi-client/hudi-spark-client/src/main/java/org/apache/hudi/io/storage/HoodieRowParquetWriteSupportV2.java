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

package org.apache.hudi.io.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;

/**
 * Hoodie Write Support for directly writing Row to Parquet.
 */
public class HoodieRowParquetWriteSupportV2 extends ParquetWriteSupport {

  private Configuration hadoopConf;

  public HoodieRowParquetWriteSupportV2(Configuration conf, StructType structType) {
    super();
    Configuration hadoopConf = new Configuration(conf);
    hadoopConf.set("spark.sql.parquet.writeLegacyFormat", "false");
    hadoopConf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS");
    this.hadoopConf = hadoopConf;
    setSchema(structType, hadoopConf);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    return new WriteSupport.FinalizedWriteContext(Collections.emptyMap());
  }

  /*public void add(String recordKey) {
    this.bloomFilter.add(recordKey);
    if (minRecordKey != null) {
      minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
    } else {
      minRecordKey = recordKey;
    }

    if (maxRecordKey != null) {
      maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
    } else {
      maxRecordKey = recordKey;
    }
  }*/
}