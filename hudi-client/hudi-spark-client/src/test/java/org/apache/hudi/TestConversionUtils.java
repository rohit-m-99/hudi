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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieDSRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieRowRecord;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY;

public class TestConversionUtils extends HoodieClientTestBase {

  private String schemaStr;
  private Schema schema;
  private StructType structType;

  public TestConversionUtils() throws IOException {
    init();
  }

  private void init() throws IOException {
    schemaStr = FileIOUtils.readAsUTFString(getClass().getResourceAsStream("/exampleSchema.txt"));
    schema = getStructTypeExampleSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
  }

  @Test
  public void testHoodieRowRecord() throws IOException {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).build();
    List<Row> rows = generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);

    Dataset<HoodieRowRecord> result = ConversionUtils.createDataset(dataset, schema, "testStruct", "testNamespace");
    List<HoodieRowRecord> resultList = result.collectAsList();
    System.out.println("size :: " + resultList.size());

    /*for(HoodieRowRecord rowRecord: resultList) {
      System.out.println("rec :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString());
    }*/
    /*for (HoodieRowRecord rowRecord : resultList) {
      for (HoodieRowRecord rowRecord2 : resultList) {
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 1 w/ 2" + rowRecord.compareTo(rowRecord2));
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 2 w/ 1 " + rowRecord2.compareTo(rowRecord));
      }
    }*/
  }

  @Test
  public void testDatasetHoodieRecordPayload() throws IOException {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).build();
    List<Row> rows = generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);

    Dataset<HoodieRecordPayload> result = ConversionUtils.createDatasetHoodieRecords(dataset, schema, "testStruct", "testNamespace");
    List<HoodieRecordPayload> resultList = result.collectAsList();
    System.out.println("size :: " + resultList.size());

    /*for(HoodieRowRecord rowRecord: resultList) {
      System.out.println("rec :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString());
    }*/
    /*for (HoodieRecordPayload rowRecord : resultList) {
      for (HoodieRecordPayload rowRecord2 : resultList) {
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 1 w/ 2" + rowRecord.compareTo(rowRecord2));
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 2 w/ 1 " + rowRecord2.compareTo(rowRecord));
      }
    }*/
  }

  @Test
  public void testHoodieDSRecord() throws IOException {
    HoodieWriteConfig config = getConfigBuilder(schemaStr).withProps(getPropsAllSet()).build();
    List<Row> rows = generateRandomRows(10);
    Dataset<Row> dataset = sqlContext.createDataFrame(rows, structType);

    Dataset<HoodieDSRecord> result = ConversionUtils.createHoodieDSRecordDataset(dataset, schema, "testStruct", "testNamespace");
    List<HoodieDSRecord> resultList = result.collectAsList();
    System.out.println("size :: " + resultList.size());

    /*for(HoodieRowRecord rowRecord: resultList) {
      System.out.println("rec :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString());
    }*/
    for (HoodieDSRecord rowRecord : resultList) {
      for (HoodieDSRecord rowRecord2 : resultList) {
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 1 w/ 2" + rowRecord.preCombine(rowRecord2));
        System.out.println("rec1 :: " + HoodieAvroUtils.bytesToAvro(rowRecord.getContent(), schema).toString() + ", rec2 " + HoodieAvroUtils.bytesToAvro(rowRecord2.getContent(), schema).toString()
            + " :: com val 2 w/ 1 " + rowRecord2.preCombine(rowRecord));
      }
    }
  }

  private Map<String, String> getPropsAllSet() {
    return getProps(true, true, true, true);
  }

  private Map<String, String> getProps(boolean setAll, boolean setKeyGen, boolean setRecordKey, boolean setPartitionPath) {
    Map<String, String> props = new HashMap<>();
    if (setAll) {
      props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
      props.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
      props.put(PARTITIONPATH_FIELD_OPT_KEY, "partition");
    } else {
      if (setKeyGen) {
        props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
      }
      if (setRecordKey) {
        props.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key");
      }
      if (setPartitionPath) {
        props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition");
      }
    }
    return props;
  }

  public static Schema getStructTypeExampleSchema() throws IOException {
    return new Schema.Parser().parse(FileIOUtils.readAsUTFString(TestConversionUtils.class.getResourceAsStream("/exampleSchema.txt")));
  }

  public static List<Row> generateRandomRows(int count) {
    Random random = new Random();
    List<Row> toReturn = new ArrayList<>();
    List<String> partitions = Arrays.asList(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH});
    for (int i = 0; i < count; i++) {
      Object[] values = new Object[3];
      values[0] = UUID.randomUUID().toString();
      values[1] = partitions.get(random.nextInt(3));
      values[2] = new Date().getTime();
      toReturn.add(RowFactory.create(values));
    }
    return toReturn;
  }

}
