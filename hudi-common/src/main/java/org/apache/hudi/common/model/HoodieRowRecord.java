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

package org.apache.hudi.common.model;

import java.io.Serializable;

public class HoodieRowRecord<T> implements Serializable {

  // only byte[] will be written to disk and not any other instance fields in this class.
  private byte[] content;

  // all meta fields that is required from driver to executor w/o reading into byte[]
  private String recordKey;
  private String partitionPath;
  private String location;
  private Comparable<T> orderingVal; // to avoid reading from byte[] and to avoid conversion to avro record, but couldn't make T extend Comprable. so need some work to be done.

  public HoodieRowRecord() {
  }

  public HoodieRowRecord preCombine(HoodieRowRecord another) {
    if (((Comparable)this.orderingVal).compareTo(another.getOrderingVal()) >= 0) {
      return this;
    }
    return another;
  }

  public HoodieRowRecord(byte[] content) {
    this.content = content;
  }

  public byte[] getContent() {
    return content;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public String getRecordKey() {
    return recordKey;
  }

  public void setRecordKey(String recordKey) {
    this.recordKey = recordKey;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public Comparable<T> getOrderingVal() {
    return orderingVal;
  }

  public void setOrderingVal(Comparable<T> orderingVal) {
    this.orderingVal = orderingVal;
  }

  /*@Override
  public int compareTo(Object other) {
    return this.orderingVal.compareTo(((HoodieRowRecord)other).orderingVal);
  }*/

  /*public int compareTo(Object o) {
    HoodieRowRecord other = (HoodieRowRecord)o;
    return ((Comparable)this.orderingVal).compareTo((Comparable)other.getOrderingVal());
  }*/
}