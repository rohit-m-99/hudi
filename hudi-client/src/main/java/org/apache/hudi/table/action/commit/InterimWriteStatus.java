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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple3;

public class InterimWriteStatus implements Serializable {

  private static final long serialVersionUID = 1L;
  private String recordKeyProp;

  public String fileId;
  public String partitionPath;
  public List<Row> successRows = new ArrayList<>();
  public List<Tuple3<Row, String, Throwable>> failedRows = new ArrayList<>();
  public Throwable globalError;
  public Path path;
  public long endTime;
  public long recordsWritten;
  public long insertRecordsWritten;

  public InterimWriteStatus() {
  }

  public InterimWriteStatus(String recordKeyProp) {
    this.recordKeyProp = recordKeyProp;
  }

  public void markSuccess(Row row) {
    this.successRows.add(row);
  }

  public void markFailure(Row row, Throwable t, Option<Map<String, String>> optionalRecordMetadata) {
    // Guaranteed to have at-least one error
    failedRows.add(new Tuple3<>(row, row.getAs(recordKeyProp), t));
  }

  public void markFailure(Row row, String recordKey, Throwable t) {
    // Guaranteed to have at-least one error
    failedRows.add(new Tuple3<>(row, recordKey, t));
  }

  @Override
  public String toString() {
    return "PartitionPath " + partitionPath + ", FileID " + fileId + ", Success records " + successRows.size()
        + ", errored Rows " + failedRows.size() + ", global error " + (globalError != null) + ", end time " + endTime;
  }
}
