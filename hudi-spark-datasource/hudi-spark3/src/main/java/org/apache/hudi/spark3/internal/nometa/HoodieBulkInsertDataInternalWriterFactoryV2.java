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

package org.apache.hudi.spark3.internal.nometa;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.spark3.internal.HoodieBulkInsertDataInternalWriter;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Factory to assist in instantiating {@link HoodieBulkInsertDataInternalWriter}.
 */
public class HoodieBulkInsertDataInternalWriterFactoryV2 implements DataWriterFactory {

  private final String instantTime;
  private final HoodieTable hoodieTable;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final int partitionFieldIndex;

  public HoodieBulkInsertDataInternalWriterFactoryV2(HoodieTable hoodieTable, HoodieWriteConfig writeConfig,
                                                     String instantTime, StructType structType, int partitionFieldIndex) {
    this.hoodieTable = hoodieTable;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.structType = structType;
    this.partitionFieldIndex = partitionFieldIndex;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new HoodieBulkInsertDataInternalWriterV2(hoodieTable, writeConfig, instantTime, partitionId, taskId,
        structType, partitionFieldIndex);
  }
}
