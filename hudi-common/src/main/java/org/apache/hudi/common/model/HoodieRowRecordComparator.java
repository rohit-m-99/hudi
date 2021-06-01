package org.apache.hudi.common.model;

import java.io.Serializable;
import java.util.Comparator;

public class HoodieRowRecordComparator implements Serializable, Comparator<HoodieRowRecord> {

  @Override
  public int compare(HoodieRowRecord o1, HoodieRowRecord o2) {
    return ((Comparable)o1.getOrderingVal()).compareTo(o2.getOrderingVal());
  }
}