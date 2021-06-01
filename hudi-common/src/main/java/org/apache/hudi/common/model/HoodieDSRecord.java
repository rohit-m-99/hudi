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

public class HoodieDSRecord<T> implements Serializable {

  // only byte[] will be written to disk and not any other instance fields in this class.
  private byte[] content;

  private HoodieKey key;

  private HoodieRecordLocation currentLocation;

  private HoodieRecordLocation newLocation;
  // private boolean sealed;
  private T orderingVal;

  public HoodieDSRecord() {
  }

  public HoodieDSRecord preCombine(HoodieDSRecord another) {
    if(((Comparable)this.orderingVal).compareTo(another.getOrderingVal()) >= 0) {
      return this;
    }
    return another;
  }

  public HoodieDSRecord(byte[] content) {
    this.content = content;
  }

  public HoodieKey getKey() {
    return key;
  }

  public void setKey(HoodieKey key) {
    this.key = key;
  }

  public HoodieRecordLocation getCurrentLocation() {
    return currentLocation;
  }

  public void setCurrentLocation(HoodieRecordLocation currentLocation) {
    this.currentLocation = currentLocation;
  }

  public HoodieRecordLocation getNewLocation() {
    return newLocation;
  }

  public void setNewLocation(HoodieRecordLocation newLocation) {
    this.newLocation = newLocation;
  }

  public byte[] getContent() {
    return content;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public T getOrderingVal() {
    return orderingVal;
  }

  public void setOrderingVal(T orderingVal) {
    this.orderingVal = orderingVal;
  }

  /*@Override
  public int compareTo(Object o) {
    return 0;
  }*/

  /*public int compareTo(Object o) {
    HoodieRowRecord other = (HoodieRowRecord)o;
    return ((Comparable)this.orderingVal).compareTo((Comparable)other.getOrderingVal());
  }*/
}
