package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.utilities.sources.TestJsonDFSSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

public class TestAvroConvertors extends UtilitiesTestBase {

  private HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

  @Test
  public void simpleAroConverterTest() throws Exception {
    List<HoodieRecord> records = dataGenerator.generateInserts("001",1);
    String[] jsonRecords = Helpers.jsonifyRecords(records);

    AvroConvertor avroConvertor = new AvroConvertor(HoodieTestDataGenerator.AVRO_SCHEMA);
    int count = 0;
    /*for(String str: jsonRecords) {
      GenericRecord genericRecord = avroConvertor.fromJson(str);
      //System.out.println("GenRec " + genericRecord);
      GenericRecord expectedRec = (GenericRecord) records.get(count++).getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA).get();
      //System.out.println("Original hoodie rec " +
        //  (expectedRec).toString());
      assertEquals(genericRecord, expectedRec);
    }*/

    avroConvertor = new AvroConvertor(HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS);
    count = 0;
    for(String str: jsonRecords) {
      GenericRecord genericRecord = avroConvertor.fromJson(str);
      System.out.println("Acutal GenRec schema "+ genericRecord.getSchema().toString());
      System.out.println("GenRec " + genericRecord);
      GenericRecord expectedRec = (GenericRecord) records.get(count++).getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA).get();
      System.out.println("Original hoodie rec " +
          (expectedRec).toString());
      System.out.println("expected GenRec schema " + expectedRec.getSchema().toString()+"\n\n\n");
      //assertEquals(genericRecord, expectedRec);
    }
  }
}
