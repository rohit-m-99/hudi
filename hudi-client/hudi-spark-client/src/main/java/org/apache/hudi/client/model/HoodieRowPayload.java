package org.apache.hudi.client.model;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

public class HoodieRowPayload implements HoodieRecordPayload<Row, Row, StructType> {

  private Row row;
  /**
   * For purposes of preCombining.
   */
  protected String orderingField;

  public HoodieRowPayload(Row row, String orderingField) {
    this.row = row;
    this.orderingField = orderingField;
  }

  @Override
  public Row preCombine(Row another) {
    int compareVal = ((Comparable) this.row.getAs(orderingField)).compareTo(another.getAs(orderingField));
    if (compareVal >= 0) {
      return this.row;
    } else {
      return another;
    }
  }

  @Override
  public Option<Row> combineAndGetUpdateValue(Row currentValue, StructType schema) throws IOException {
    return Option.of(this.row);
  }

  @Override
  public Option<Row> getInsertValue(StructType schema) throws IOException {
    return Option.of(this.row);
  }

}
