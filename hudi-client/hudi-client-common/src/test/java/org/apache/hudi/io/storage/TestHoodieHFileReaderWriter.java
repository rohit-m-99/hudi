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

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.TaskContextSupplier;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieHFileReaderWriter {
  private final Path filePath = new Path(System.getProperty("java.io.tmpdir") + "/f1_1-0-1_000.hfile");

  private static final String ALPHA_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
  private static final int ALPHA_CHARS_LENGTH = ALPHA_CHARS.length();

  private static final ThreadLocal<CharsetEncoder> ENCODER =
      ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);
  private static final ThreadLocal<CharsetDecoder> DECODER =
      ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);

  @BeforeEach
  @AfterEach
  public void clearTempFile() {
    File file = new File(filePath.toString());
    if (file.exists()) {
      file.delete();
    }
  }

  private HoodieHFileWriter createHFileWriter(Schema avroSchema) throws Exception {
    return createHFileWriter(avroSchema, filePath);
  }

  private HoodieHFileWriter createHFileWriter(Schema avroSchema, Path filePath) throws Exception {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    Configuration conf = new Configuration();
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    String instantTime = "000";

    HoodieHFileConfig hoodieHFileConfig = new HoodieHFileConfig(conf, Compression.Algorithm.NONE, 64 * 1024, 120 * 1024 * 1024,
        null);
    return new HoodieHFileWriter(instantTime, filePath, hoodieHFileConfig, avroSchema, mockTaskContextSupplier);
  }

  @Test
  public void testWriteReadHFile() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = String.format("%s%04d", "key", i);
      record.put("_row_key", key);
      keys.add(key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      writer.writeAvro(key, record);
      recordMap.put(key, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HoodieHFileReader hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
    List<Pair<String, IndexedRecord>> records = hoodieHFileReader.readAllRecords();
    records.forEach(entry -> assertEquals(entry.getSecond(), recordMap.get(entry.getFirst())));
    hoodieHFileReader.close();

    for (int i = 0; i < 20; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(50);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);
      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);
      hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
      List<Pair<String, GenericRecord>> result = hoodieHFileReader.readRecordsByKey(rowsList);
      assertEquals(result.size(), randomRowstoFetch);
      result.forEach(entry -> {
        assertEquals(entry.getSecond(), recordMap.get(entry.getFirst()));
      });
      hoodieHFileReader.close();
    }
  }

  /*@Test
  public void testWriteReadHFileMetadataPayload() throws Exception {
    //Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"string\",\n" +
        "      \"avro.java.string\" : \"String\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new TreeMap<>();

    List<String> keys2 = new ArrayList<>();
    Map<String, GenericRecord> recordMap2 = new TreeMap<>();

    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 100;
    int numFiles = 500;
    int partitionIndex = 0;
    int colIndex = 0;
    int fileIndex = 0;
    for (int i = 0; i < numPartitions; i++) {
      //String partitionName = generateRandomChars(10);
      String partitionName = "";
      for (int j = 0; j < numCols; j++) {
        String colName = null;
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          colName = stringIdPrefix + String.format("%04d", i);
          isString = true;
        } else if (j % 5 == 1) {
          colName = longIdPrefix + String.format("%04d", i);
          isLong = true;
        } else if (j % 5 == 2) {
          colName = intIdPrefix + String.format("%04d", i);
          isInteger = true;
        } else if (j % 5 == 3) {
          colName = doubleIdPrefix + String.format("%04d", i);
          isDouble = true;
        } else {
          colName = booleanIdPrefix + String.format("%04d", i);
          isBoolean = true;
        }
        //String colName = generateRandomChars(15);
        fileIndex = 0;
        for (int k = 0; k < numFiles; k++) {
          String fileId = UUID.randomUUID().toString();
          String writeToken = "23_04_235";
          String commitTime = "20211015191547";
          String fileName = fileId + "_" + writeToken + "_" + commitTime + ".parquet";
          //System.out.println("file length " + fileName.length());
          String key = partitionName + "_" + colName + "_" + fileName;
          //System.out.println(" key length " + key.length());
          //String[] minMaxVals = getMinMaxValue(isString, isLong, isInteger, isDouble, isBoolean);
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          //String minValue = UUID.randomUUID().toString();
          //String maxValue = UUID.randomUUID().toString();
          GenericRecord record = new GenericData.Record(avroSchema);
          record.put("key", key);
          //record.put("type", 1);
          /*record.put("minValue", minMaxVals[0]);
          record.put("maxValue", minMaxVals[1]);
          record.put("sizeOnDisk", sizeOnDisk);
          record.put("numVals", numVals);
          record.put("nullVals", nullVals);
          record.put("nanVals", nanVals);*/
  //byte[] minVal = minMaxVals[0].array();

  //record.put("mnv", minMaxVals[0]);
  //record.put("mxv", minMaxVals[0]);

  /*record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);

          recordMap.put(key, record);
          keys.add(key);

          GenericRecord record2 = new GenericData.Record(avroSchema);
          key = (colIndex++) + "_" + (fileIndex++);
          //System.out.println(" Encoded key length " + key.length());
          record2.put("key", key);
          //record2.put("type", 1);
          /*record2.put("minValue", minMaxVals[0]);
          record2.put("maxValue", minMaxVals[1]);
          record2.put("sizeOnDisk", sizeOnDisk);
          record2.put("numVals", numVals);
          record2.put("nullVals", nullVals);
          record2.put("nanVals", nanVals);*/

  //record2.put("mnv", minMaxVals[0]);
  //record2.put("mxv", minMaxVals[1]);
  /*record2.put("mnv", ByteBuffer.wrap(minVal));
          record2.put("mxv", ByteBuffer.wrap(maxVal));
          record2.put("sd", sizeOnDisk);
          record2.put("tv", numVals);
          record2.put("nlv", nullVals);
          record2.put("nnv", nanVals);
          recordMap2.put(key, record2);
          keys2.add(key);
        }
      }
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(filePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);

    new File(filePath.toString()).delete();

    writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap2.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    fileLength = new File(filePath.toString()).length();
    System.out.println("Encoded file length :: " + fileLength);

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    // Serialize users to disk
    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (GenericRecord user : recordMap2.values()) {
        fileWriter.append(user);
      }
      fileWriter.close();
    }
    System.out.println("Avro file length :: " + new File(avroPath).length());

    //new File(filePath.toString()).delete();


    /*Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HoodieHFileReader hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
    List<Pair<String, IndexedRecord>> records = hoodieHFileReader.readAllRecords();
    records.forEach(entry -> assertEquals(entry.getSecond(), recordMap.get(entry.getFirst())));
    hoodieHFileReader.close();

    for (int i = 0; i < 1; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);
      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);
      hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
      List<Pair<String, GenericRecord>> result = hoodieHFileReader.readRecordsByKey(rowsList);
      assertEquals(result.size(), randomRowstoFetch);
      result.forEach(entry -> {
        assertEquals(entry.getSecond(), recordMap.get(entry.getFirst()));
      });
      hoodieHFileReader.close();
    }*/

  //}

  /*@Test
  public void testWriteReadHFileMetadataPayloadCustomKey() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "    \"type\": \"bytes\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    int colIndex = 0;
    int fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < numPartitions; i++) {
        //String partitionName = generateRandomChars(10);
        colIndex = 0;
        for (int j = 0; j < numCols; j++) {
          String colName = null;
          boolean isString = false;
          boolean isLong = false;
          boolean isInteger = false;
          boolean isDouble = false;
          boolean isBoolean = false;
          if (j % 5 == 0) {
            colName = stringIdPrefix + String.format("%04d", i);
            isString = true;
          } else if (j % 5 == 1) {
            colName = longIdPrefix + String.format("%04d", i);
            isLong = true;
          } else if (j % 5 == 2) {
            colName = intIdPrefix + String.format("%04d", i);
            isInteger = true;
          } else if (j % 5 == 3) {
            colName = doubleIdPrefix + String.format("%04d", i);
            isDouble = true;
          } else {
            colName = booleanIdPrefix + String.format("%04d", i);
            isBoolean = true;
          }
          fileIndex = 0;
          for (int k = 0; k < numFiles; k++) {
            ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
            byte[] minVal = minMaxVals[0].array();
            byte[] maxVal = minMaxVals[1].array();
            long sizeOnDisk = 10 + RANDOM.nextInt(1000);
            long numVals = 10 + RANDOM.nextInt(1000);
            long nullVals = 10 + RANDOM.nextInt(200);
            long nanVals = 10 + RANDOM.nextInt(300);
            GenericRecord record = new GenericData.Record(avroSchema);

            ByteBuffer key = getKeyAsByteBuffer(colIndex++, commitTime, fileIndex++);
            int len = key.array().length;
            record.put("key", key);
            record.put("mnv", ByteBuffer.wrap(minVal));
            record.put("mxv", ByteBuffer.wrap(maxVal));
            record.put("sd", sizeOnDisk);
            record.put("tv", numVals);
            record.put("nlv", nullVals);
            record.put("nnv", nanVals);
            fileWriter.append(record);
          }
        }
      }
      fileWriter.close();
    }
    System.out.println("Avro file length null codec :: " + new File(avroPath).length());
  }

  @Test
  public void testWriteReadHFileMetadataPayloadCustomKey1() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "    \"type\": \"bytes\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    int colIndex = 0;
    int fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < numPartitions; i++) {
        String partitionName = generateRandomChars(10);
        colIndex = 0;
        for (int j = 0; j < numCols; j++) {
          String colName = null;
          boolean isString = false;
          boolean isLong = false;
          boolean isInteger = false;
          boolean isDouble = false;
          boolean isBoolean = false;
          if (j % 5 == 0) {
            colName = stringIdPrefix + String.format("%04d", i);
            isString = true;
          } else if (j % 5 == 1) {
            colName = longIdPrefix + String.format("%04d", i);
            isLong = true;
          } else if (j % 5 == 2) {
            colName = intIdPrefix + String.format("%04d", i);
            isInteger = true;
          } else if (j % 5 == 3) {
            colName = doubleIdPrefix + String.format("%04d", i);
            isDouble = true;
          } else {
            colName = booleanIdPrefix + String.format("%04d", i);
            isBoolean = true;
          }
          fileIndex = 0;
          for (int k = 0; k < numFiles; k++) {
            ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
            byte[] minVal = minMaxVals[0].array();
            byte[] maxVal = minMaxVals[1].array();
            long sizeOnDisk = 10 + RANDOM.nextInt(1000);
            long numVals = 10 + RANDOM.nextInt(1000);
            long nullVals = 10 + RANDOM.nextInt(200);
            long nanVals = 10 + RANDOM.nextInt(300);
            GenericRecord record = new GenericData.Record(avroSchema);

            ByteBuffer key = getKeyAsByteBuffer(colIndex++, commitTime, fileIndex++);
            int len = key.array().length;
            record.put("key", key);
            record.put("mnv", ByteBuffer.wrap(minVal));
            record.put("mxv", ByteBuffer.wrap(maxVal));
            record.put("sd", sizeOnDisk);
            record.put("tv", numVals);
            record.put("nlv", nullVals);
            record.put("nnv", nanVals);
            fileWriter.append(record);
          }
        }
      }
      fileWriter.close();
    }
    System.out.println("Avro file length null codec :: " + new File(avroPath).length());
  }

  @Test
  public void testWriteReadHFileMetadataPayloadCustomKey2() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "    \"type\": \"bytes\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    int colIndex = 0;
    int fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < numPartitions; i++) {
        String partitionName = generateRandomChars(10);
        colIndex = 0;
        for (int j = 0; j < numCols; j++) {
          String colName = null;
          boolean isString = false;
          boolean isLong = false;
          boolean isInteger = false;
          boolean isDouble = false;
          boolean isBoolean = false;
          if (j % 5 == 0) {
            colName = stringIdPrefix + String.format("%04d", i);
            isString = true;
          } else if (j % 5 == 1) {
            colName = longIdPrefix + String.format("%04d", i);
            isLong = true;
          } else if (j % 5 == 2) {
            colName = intIdPrefix + String.format("%04d", i);
            isInteger = true;
          } else if (j % 5 == 3) {
            colName = doubleIdPrefix + String.format("%04d", i);
            isDouble = true;
          } else {
            colName = booleanIdPrefix + String.format("%04d", i);
            isBoolean = true;
          }
          fileIndex = 0;
          for (int k = 0; k < numFiles; k++) {
            ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
            byte[] minVal = minMaxVals[0].array();
            byte[] maxVal = minMaxVals[1].array();
            long sizeOnDisk = 10 + RANDOM.nextInt(1000);
            long numVals = 10 + RANDOM.nextInt(1000);
            long nullVals = 10 + RANDOM.nextInt(200);
            long nanVals = 10 + RANDOM.nextInt(300);
            GenericRecord record = new GenericData.Record(avroSchema);

            ByteBuffer key = getKeyAsByteBuffer(colIndex++, commitTime, fileIndex++);
            int len = key.array().length;
            record.put("key", key);
            record.put("mnv", ByteBuffer.wrap(minVal));
            record.put("mxv", ByteBuffer.wrap(maxVal));
            record.put("sd", sizeOnDisk);
            record.put("tv", numVals);
            record.put("nlv", nullVals);
            record.put("nnv", nanVals);
            fileWriter.append(record);
          }
        }
      }
      fileWriter.close();
    }
    System.out.println("Avro file length null codec :: " + new File(avroPath).length());
  }*/

  @Test
  public void testWriteReadHFileMetadataPayloadCustomKeyString() throws Exception {
    String schemaStr = "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"HoodieMetadataRecord\",\n"
        + "  \"namespace\" : \"org.apache.hudi.avro.model\",\n"
        + "  \"doc\" : \"A record saved within the Metadata Table\",\n"
        + "  \"fields\" : [ {\n"
        + "    \"name\" : \"key\", // partition_col_filename\n"
        + "    \"type\" : {\n"
        + "    \"type\": \"string\"\n"
        + "    }\n"
        + "  }, {\n"
        + "    \"name\" : \"mnv\",\n"
        + "    \"type\": \"bytes\"\n"
        + "  }, {\n"
        + "    \"name\" : \"mxv\",\n"
        + "    \"type\": \"bytes\"\n"
        + "  }, {\n"
        + "    \"name\" : \"sd\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"tv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"nlv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"nnv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;
    System.out.println(" commit time length " + Long.toString(commitTime).length());

    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < numPartitions; i++) {
        String partitionName = generateRandomChars(10);
        for (int j = 0; j < numCols; j++) {
          String colName = null;
          boolean isString = false;
          boolean isLong = false;
          boolean isInteger = false;
          boolean isDouble = false;
          boolean isBoolean = false;
          if (j % 5 == 0) {
            colName = stringIdPrefix + String.format("%04d", i);
            isString = true;
          } else if (j % 5 == 1) {
            colName = longIdPrefix + String.format("%04d", i);
            isLong = true;
          } else if (j % 5 == 2) {
            colName = intIdPrefix + String.format("%04d", i);
            isInteger = true;
          } else if (j % 5 == 3) {
            colName = doubleIdPrefix + String.format("%04d", i);
            isDouble = true;
          } else {
            colName = booleanIdPrefix + String.format("%04d", i);
            isBoolean = true;
          }
          for (int k = 0; k < numFiles; k++) {
            ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
            byte[] minVal = minMaxVals[0].array();
            byte[] maxVal = minMaxVals[1].array();
            long sizeOnDisk = 10 + RANDOM.nextInt(1000);
            long numVals = 10 + RANDOM.nextInt(1000);
            long nullVals = 10 + RANDOM.nextInt(200);
            long nanVals = 10 + RANDOM.nextInt(300);
            GenericRecord record = new GenericData.Record(avroSchema);

            String key = Integer.toString(j) + Long.toString(commitTime) + Integer.toString(k);
            System.out.println("Key length " + key.length() + " " + key);
            record.put("key", key);
            record.put("mnv", ByteBuffer.wrap(minVal));
            record.put("mxv", ByteBuffer.wrap(maxVal));
            record.put("sd", sizeOnDisk);
            record.put("tv", numVals);
            record.put("nlv", nullVals);
            record.put("nnv", nanVals);
            fileWriter.append(record);
          }
        }
      }
      fileWriter.close();
    }
    System.out.println("Avro file length null codec :: " + new File(avroPath).length());
  }

  @Test
  public void testWriteReadHFileMetadataPayloadCustomKeyStringHFile() throws Exception {
    String schemaStr = "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"HoodieMetadataRecord\",\n"
        + "  \"namespace\" : \"org.apache.hudi.avro.model\",\n"
        + "  \"doc\" : \"A record saved within the Metadata Table\",\n"
        + "  \"fields\" : [ {\n"
        + "    \"name\" : \"mnv\",\n"
        + "    \"type\": \"bytes\"\n"
        + "  }, {\n"
        + "    \"name\" : \"mxv\",\n"
        + "    \"type\": \"bytes\"\n"
        + "  }, {\n"
        + "    \"name\" : \"sd\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"tv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"nlv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }, {\n"
        + "    \"name\" : \"nnv\",\n"
        + "    \"type\" : [ \"null\", \"long\" ],\n"
        + "    \"doc\" : \"\",\n"
        + "    \"default\" : null\n"
        + "  }]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;

    String hFileName = UUID.randomUUID().toString() + ".hfile";
    String hFilePath = filePath.getParent().toString() + "/" + hFileName;

    long commitTime = 20211015191547000L;
    long commitTime1 = 20211013094547000L;
    long commitTime2 = 20211015191541094L;

    Map<String, GenericRecord> recordMap = new TreeMap<>();

    for (int i = 0; i < numPartitions; i++) {
      String partitionName = generateRandomChars(10);
      for (int j = 0; j < numCols; j++) {
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          isString = true;
        } else if (j % 5 == 1) {
          isLong = true;
        } else if (j % 5 == 2) {
          isInteger = true;
        } else if (j % 5 == 3) {
          isDouble = true;
        } else {
          isBoolean = true;
        }
        for (int k = 0; k < numFiles; k++) {
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          GenericRecord record = new GenericData.Record(avroSchema);
          String key = Long.toString(j) + Long.toString(commitTime) + Long.toString(k);
          record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);
          recordMap.put(key, record);
        }
      }
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema, new Path(hFilePath));
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(hFilePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);
    System.out.println("hfile name " + hFilePath.toString());

    /*List<String> keys = new ArrayList<>(recordMap.keySet());
    Collections.shuffle(keys);
    List<String> keysToLookup = keys.subList(0, 1000);
    Collections.sort(keysToLookup);

    String randomKeyFile = UUID.randomUUID().toString();
    File keyFile = new File(avroFile.getParent(), randomKeyFile);
    FileWriter keyWriter = new FileWriter(keyFile);
    for(String str: keysToLookup) {
      keyWriter.write(str + System.lineSeparator());
    }
    keyWriter.close();

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HoodieHFileReader hfileReader = new HoodieHFileReader(conf, new Path(filePath.toString()), cacheConfig);

    int totalKeys = keysToLookup.size();
    long startTimeMs = System.currentTimeMillis();
    List<Pair<String, GenericRecord>> result = hfileReader.readRecordsByKey(keysToLookup, avroSchema);
    System.out.println("Time taken to look up " + totalKeys + " = " + (System.currentTimeMillis() - startTimeMs));
    System.out.println("File location :: " + filePath.toString());
    startTimeMs = System.currentTimeMillis();
    result = hfileReader.getRecordsByKeyPrefix("195", avroSchema);
    System.out.println("Prefix look up of one key " + (System.currentTimeMillis() - startTimeMs) + ", total entries " + result.size());
    */
    System.out.println("...");
  }

  /*@Test
  public void testWriteReadHFileTemplocal() throws Exception {

    String randomFileName = UUID.randomUUID().toString();
    String basePath = "/tmp/hfile/";
    String filePathStr = basePath + randomFileName;

    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;
    long commitTime1 = 20211013094547000L;
    long commitTime2 = 20211015191541094L;

    Map<String, GenericRecord> recordMap = new TreeMap<>();
    Map<String, LLLtriplet> keyMap = new HashMap<>();

    for (int i = 0; i < numPartitions; i++) {
      String partitionName = generateRandomChars(10);
      for (int j = 0; j < numCols; j++) {
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          isString = true;
        } else if (j % 5 == 1) {
          isLong = true;
        } else if (j % 5 == 2) {
          isInteger = true;
        } else if (j % 5 == 3) {
          isDouble = true;
        } else {
          isBoolean = true;
        }
        for (int k = 0; k < numFiles; k++) {
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          GenericRecord record = new GenericData.Record(avroSchema);
          String key = Long.toString(j) + Long.toString(commitTime) + Long.toString(k);
          record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);
          recordMap.put(key, record);
        }
      }
    }

    String randomHFile = UUID.randomUUID().toString() + ".hfile";
    HoodieHFileWriter writer = createHFileWriter(avroSchema, new Path(basePath + "/" + randomHFile));
    System.out.println("Hfile name :: " + (basePath + randomHFile));
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(basePath + "/" + randomHFile).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);

    System.out.println("...");
  }

  @Test
  public void testKeyStringHFileValueAdditional() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"string\",\n" +
        "      \"avro.java.string\" : \"String\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;
    long commitTime1 = 20211013094547000L;
    long commitTime2 = 20211015191541094L;

    Map<String, GenericRecord> recordMap = new TreeMap<>();
    Map<String, LLLtriplet> keyMap = new HashMap<>();

    for (int i = 0; i < numPartitions; i++) {
      String partitionName = generateRandomChars(10);
      for (int j = 0; j < numCols; j++) {
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          isString = true;
        } else if (j % 5 == 1) {
          isLong = true;
        } else if (j % 5 == 2) {
          isInteger = true;
        } else if (j % 5 == 3) {
          isDouble = true;
        } else {
          isBoolean = true;
        }
        for (int k = 0; k < numFiles; k++) {
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          GenericRecord record = new GenericData.Record(avroSchema);
          String key = Long.toString(j) + Long.toString(commitTime) + Long.toString(k);
          record.put("key", generateRandomChars(20));
          record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);
          recordMap.put(key, record);
        }
      }
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(filePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);

    /*List<String> keys = new ArrayList<>(recordMap.keySet());
    Collections.shuffle(keys);
    List<String> keysToLookup = keys.subList(0, 1000);
    Collections.sort(keysToLookup);

    String randomKeyFile = UUID.randomUUID().toString();
    File keyFile = new File(avroFile.getParent(), randomKeyFile);
    FileWriter keyWriter = new FileWriter(keyFile);
    for(String str: keysToLookup) {
      keyWriter.write(str + System.lineSeparator());
    }
    keyWriter.close();

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HoodieHFileReader hfileReader = new HoodieHFileReader(conf, new Path(filePath.toString()), cacheConfig);

    int totalKeys = keysToLookup.size();
    long startTimeMs = System.currentTimeMillis();
    List<Pair<String, GenericRecord>> result = hfileReader.readRecordsByKey(keysToLookup, avroSchema);
    System.out.println("Time taken to look up " + totalKeys + " = " + (System.currentTimeMillis() - startTimeMs));

    String firstKey = keysToLookup.get(0);
    String lookUpKey = firstKey.substring(0, 6);
    startTimeMs = System.currentTimeMillis();
    result = hfileReader.getRecordsByKeyPrefix(lookUpKey, avroSchema);
    System.out.println("Prefix look up of one key " + (System.currentTimeMillis() - startTimeMs) + ", total entries " + result.size());

    System.out.println("...");*/
  //}

  /*@Test
  public void testWriteReadHFileMetadataPayloadCustomKeyLLL() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "    \"type\": \"bytes\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    long colIndex = 0;
    long fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < numPartitions; i++) {
        long partitionId = RANDOM.nextInt(1000);
        colIndex = 0;
        for (int j = 0; j < numCols; j++) {
          String colName = null;
          boolean isString = false;
          boolean isLong = false;
          boolean isInteger = false;
          boolean isDouble = false;
          boolean isBoolean = false;
          if (j % 5 == 0) {
            colName = stringIdPrefix + String.format("%04d", i);
            isString = true;
          } else if (j % 5 == 1) {
            colName = longIdPrefix + String.format("%04d", i);
            isLong = true;
          } else if (j % 5 == 2) {
            colName = intIdPrefix + String.format("%04d", i);
            isInteger = true;
          } else if (j % 5 == 3) {
            colName = doubleIdPrefix + String.format("%04d", i);
            isDouble = true;
          } else {
            colName = booleanIdPrefix + String.format("%04d", i);
            isBoolean = true;
          }
          fileIndex = 0;
          for (int k = 0; k < numFiles; k++) {
            ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
            byte[] minVal = minMaxVals[0].array();
            byte[] maxVal = minMaxVals[1].array();
            long sizeOnDisk = 10 + RANDOM.nextInt(1000);
            long numVals = 10 + RANDOM.nextInt(1000);
            long nullVals = 10 + RANDOM.nextInt(200);
            long nanVals = 10 + RANDOM.nextInt(300);
            GenericRecord record = new GenericData.Record(avroSchema);

            ByteBuffer key = getKeyAsByteBuffer(colIndex++, partitionId, fileIndex++);
            int len = key.array().length;
            record.put("key", key);
            record.put("mnv", ByteBuffer.wrap(minVal));
            record.put("mxv", ByteBuffer.wrap(maxVal));
            record.put("sd", sizeOnDisk);
            record.put("tv", numVals);
            record.put("nlv", nullVals);
            record.put("nnv", nanVals);
            fileWriter.append(record);
          }
        }
      }
      fileWriter.close();
    }
    System.out.println("Avro file length null codec :: " + new File(avroPath).length());
  }


  @Test
  public void testMetadataPayloadCustomKeyStringHFileLLL() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    long colIndex = 0;
    long fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    Map<String, GenericRecord> recordMap = new TreeMap<>();

    for (int i = 0; i < numPartitions; i++) {
      long partitionId = RANDOM.nextInt(10000);
      colIndex = 0;
      for (int j = 0; j < numCols; j++) {
        String colName = null;
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          colName = stringIdPrefix + String.format("%04d", i);
          isString = true;
        } else if (j % 5 == 1) {
          colName = longIdPrefix + String.format("%04d", i);
          isLong = true;
        } else if (j % 5 == 2) {
          colName = intIdPrefix + String.format("%04d", i);
          isInteger = true;
        } else if (j % 5 == 3) {
          colName = doubleIdPrefix + String.format("%04d", i);
          isDouble = true;
        } else {
          colName = booleanIdPrefix + String.format("%04d", i);
          isBoolean = true;
        }
        fileIndex = 0;
        for (int k = 0; k < numFiles; k++) {
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          GenericRecord record = new GenericData.Record(avroSchema);

          //String key = Long.toString(colIndex++) + "" + Long.toString(commitTime) + Long.toString(fileIndex++);
          //LLLtriplet llLtriplet = new LLLtriplet((colIndex++), partitionId, (fileIndex++));
          //ByteBuffer key = getKeyAsByteBuffer(colIndex++, partitionId, fileIndex++);
          String key = Long.toString(colIndex++) + "" + Long.toString(partitionId) + Long.toString((fileIndex++));
          //Comparator colIdComaparator = Comparator.comparing(LLLtriplet::getColId);
          //Comparator partIdComaparator = Comparator.comparing(LLLtriplet::getPartitionId);
          //Comparator fileIdComaparator = Comparator.comparing(LLLtriplet::getFileId);

          record.put("key", key);
          record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);
          recordMap.put(key, record);
        }
      }
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(filePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);
  }


  @Test
  public void testMetadataPayloadCustomKeyStringHFileLLLNonStringKey() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "    \"type\": \"string\"\n" +
        "    }\n" +
        "  }, {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    String hFileSchemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"mnv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"mxv\",\n" +
        "    \"type\": \"bytes\"\n" +
        "  }, {\n" +
        "    \"name\" : \"sd\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"tv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nlv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }, {\n" +
        "    \"name\" : \"nnv\",\n" +
        "    \"type\" : [ \"null\", \"long\" ],\n" +
        "    \"doc\" : \"\",\n" +
        "    \"default\" : null\n" +
        "  }]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(hFileSchemaStr);
    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 500;
    int numFiles = 100;
    int partitionIndex = 0;
    long colIndex = 0;
    long fileIndex = 0;

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    long commitTime = 20211015191547000L;

    Comparator colIdComaparator = Comparator.comparing(LLLtriplet::getColId);
    Comparator partIdComaparator = Comparator.comparing(LLLtriplet::getPartitionId);
    Comparator fileIdComaparator = Comparator.comparing(LLLtriplet::getFileId);
    Comparator tripletComparator = colIdComaparator.thenComparing(partIdComaparator).thenComparing(fileIdComaparator);

    Map<LLLtriplet, GenericRecord> recordMap = new TreeMap<>(tripletComparator);

    for (int i = 0; i < numPartitions; i++) {
      long partitionId = RANDOM.nextInt(10000);
      colIndex = 0;
      for (int j = 0; j < numCols; j++) {
        String colName = null;
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          colName = stringIdPrefix + String.format("%04d", i);
          isString = true;
        } else if (j % 5 == 1) {
          colName = longIdPrefix + String.format("%04d", i);
          isLong = true;
        } else if (j % 5 == 2) {
          colName = intIdPrefix + String.format("%04d", i);
          isInteger = true;
        } else if (j % 5 == 3) {
          colName = doubleIdPrefix + String.format("%04d", i);
          isDouble = true;
        } else {
          colName = booleanIdPrefix + String.format("%04d", i);
          isBoolean = true;
        }
        fileIndex = 0;
        for (int k = 0; k < numFiles; k++) {
          ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
          byte[] minVal = minMaxVals[0].array();
          byte[] maxVal = minMaxVals[1].array();
          long sizeOnDisk = 10 + RANDOM.nextInt(1000);
          long numVals = 10 + RANDOM.nextInt(1000);
          long nullVals = 10 + RANDOM.nextInt(200);
          long nanVals = 10 + RANDOM.nextInt(300);
          GenericRecord record = new GenericData.Record(avroSchema);

          //String key = Long.toString(colIndex++) + "" + Long.toString(commitTime) + Long.toString(fileIndex++);
          //ByteBuffer key = getKeyAsByteBuffer(colIndex++, partitionId, fileIndex++);
          //String key = Long.toString(colIndex++) + "" + Long.toString(partitionId) + Long.toString((fileIndex++));

          LLLtriplet llLtriplet = new LLLtriplet((colIndex++), partitionId, (fileIndex++));
          //record.put("key", llLtriplet);
          record.put("mnv", ByteBuffer.wrap(minVal));
          record.put("mxv", ByteBuffer.wrap(maxVal));
          record.put("sd", sizeOnDisk);
          record.put("tv", numVals);
          record.put("nlv", nullVals);
          record.put("nnv", nanVals);
          recordMap.put(llLtriplet, record);
        }
      }
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    for (Map.Entry<LLLtriplet, GenericRecord> entry : recordMap.entrySet()) {
      ByteBuffer keyBytes = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN).putLong(entry.getKey().colId)
          .putLong(entry.getKey().getPartitionId()).putLong(entry.getKey().getFileId());
      writer.writeAvro(keyBytes.array(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(filePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);
  }

  class LLLtriplet {
    long colId;
    long partitionId;
    long fileId;

    public LLLtriplet(long colId, long partitionId, long fileId) {
      this.colId = colId;
      this.partitionId = partitionId;
      this.fileId = fileId;
    }

    public long getColId() {
      return colId;
    }

    public long getPartitionId() {
      return partitionId;
    }

    public long getFileId() {
      return fileId;
    }
  }


  @Test
  public void testIcebergMeta1() throws IOException {
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"manifest_entry\",\"fields\":[{\"name\":\"status\",\"type\":\"int\",\"field-id\":0},{\"name\":
        \"snapshot_id\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":1},{\"name\":\"data_file\",\"type\":{\"type\":
        \"record\",\"name\":\"r2\",\"fields\":[{\"name\":\"file_path\",\"type\":\"string\",\"doc\":\"Location URI with FS scheme\",
        \"field-id\":100},{\"name\":\"file_format\",\"type\":\"string\",\"doc\":\"File format name: avro, orc, or parquet\",\"field-id
        \":101},{\"name\":\"partition\",\"type\":{\"type\":\"record\",\"name\":\"r102\",\"fields\":[]},\"field-id\":102},{\"name\":
        \"record_count\",\"type\":\"long\",\"doc\":\"Number of records in the file\",\"field-id\":103},{\"name\":\"file_size_in_bytes
        \",\"type\":\"long\",\"doc\":\"Total file size in bytes\",\"field-id\":104},{\"name\":\"block_size_in_bytes\",\"type\":\"long
        \",\"field-id\":105},{\"name\":\"column_sizes\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name
        \":\"k117_v118\",\"fields\":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":117},{\"name\":\"value\",\"type\":\"long\",
        \"field-id\":118}]},\"logicalType\":\"map\"}],\"doc\":\"Map of column id to total size on disk\",\"default\":null,\"field-id
        \":108},{\"name\":\"value_counts\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"k119_v120
        \",\"fields\":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":119},{\"name\":\"value\",\"type\":\"long\",\"field-id\":120}]}
        ,\"logicalType\":\"map\"}],\"doc\":\"Map of column id to total count, including null and NaN\",\"default\":null,\"field-id\":109}
        ,{\"name\":\"null_value_counts\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"k121_v122\",
        \"fields\":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":121},{\"name\":\"value\",\"type\":\"long\",\"field-id\":122}]},
        \"logicalType\":\"map\"}],\"doc\":\"Map of column id to null value count\",\"default\":null,\"field-id\":110},{\"name\":
        \"nan_value_counts\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"k138_v139\",\"fields
        \":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":138},{\"name\":\"value\",\"type\":\"long\",\"field-id\":139}]},
        \"logicalType\":\"map\"}],\"doc\":\"Map of column id to number of NaN values in the column\",\"default\":null,\"field-id
        \":137},{\"name\":\"lower_bounds\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":
        \"k126_v127\",\"fields\":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":126},{\"name\":\"value\",\"type\":\"bytes
        \",\"field-id\":127}]},\"logicalType\":\"map\"}],\"doc\":\"Map of column id to lower bound\",\"default\":null,
        \"field-id\":125},{\"name\":\"upper_bounds\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\
        ",\"name\":\"k129_v130\",\"fields\":[{\"name\":\"key\",\"type\":\"int\",\"field-id\":129},{\"name\":\"value\",
        \"type\":\"bytes\",\"field-id\":130}]},\"logicalType\":\"map\"}],\"doc\":\"Map of column id to upper bound\",
        \"default\":null,\"field-id\":128},{\"name\":\"key_metadata\",\"type\":[\"null\",\"bytes\"],\"doc\":
        \"Encryption key metadata blob\",\"default\":null,\"field-id\":131},{\"name\":\"split_offsets\",\"type
        \":[\"null\",{\"type\":\"array\",\"items\":\"long\",\"element-id\":133}],\"doc\":\"Splittable offsets\
        ",\"default\":null,\"field-id\":132},{\"name\":\"sort_order_id\",\"type\":[\"null\",\"int\"],\"doc\":\"Sort order
        ID\",\"default\":null,\"field-id\":140}]},\"field-id\":2}]}";
   */

  /*Schema avroSchema = new Schema.Parser().parse(schemaStr);

    //String avroFilePath = "/Users/nsb/Documents/personal/temp/iceberg_meta/iceberg_100f_500c_20k/258f66b2-2934-4fd5-b69e-83775696a601-m0.avro";
    String avroFilePath = "/Users/nsb/Documents/personal/temp/iceberg_meta/iceberg_100f_400c_20k/da1be467-6baa-4b30-92a0-c8d4c0916d19-m0.avro";
    File avroFile = new File(avroFilePath);
    DatumReader<GenericRecord> avroReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records = new ArrayList<>();
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroFile, avroReader)) {
      fileReader.forEachRemaining(genericRecord -> records.add(genericRecord));
        /*GenericRecord rec = fileReader.next();
        if (rec != null) {
          records.add(rec);
        } else {
          break;
        }
      }*/
  /*fileReader.close();
    }

    System.out.println("Sample record " + records.get(0).getSchema().toString());
    System.out.println("\n");
    System.out.println("1 rec:: " + records.get(0).toString());

    GenericRecord dataFileRec = (GenericRecord) records.get(0).get("data_file");
    GenericArray lowerBoundsArray = (GenericArray) dataFileRec.get("lower_bounds");
    GenericData.Record lowerBoundsRec0 = (GenericData.Record) lowerBoundsArray.get(0);
    //CharBuffer lowerBound0 = DECODER.get().decode((ByteBuffer) lowerBoundsRec0.get("value"));
    GenericData.Record lowerBoundsRec1 = (GenericData.Record) lowerBoundsArray.get(1);
    //CharBuffer lowerBound1 = DECODER.get().decode((ByteBuffer) lowerBoundsRec1.get("value"));
    GenericData.Record lowerBoundsRec2 = (GenericData.Record) lowerBoundsArray.get(2);
    //CharBuffer lowerBound2 = DECODER.get().decode((ByteBuffer) lowerBoundsRec2.get("value"));
    GenericData.Record lowerBoundsRec3 = (GenericData.Record) lowerBoundsArray.get(3);
    //CharBuffer lowerBound3 = DECODER.get().decode((ByteBuffer) lowerBoundsRec3.get("value"));
    GenericData.Record lowerBoundsRec4 = (GenericData.Record) lowerBoundsArray.get(4);
    //CharBuffer lowerBound4 = DECODER.get().decode((ByteBuffer) lowerBoundsRec4.get("value"));

    // System.out.println("5 values " + lowerBound0 + ", " + lowerBound1 + ", " + lowerBound2 + ", " + lowerBound3 + ", " + lowerBound4);
    // Serialize users to disk
    File avroFile2 = new File(filePath.toString());
    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.nullCodec());
      fileWriter.create(avroSchema, avroFile2);
      for (GenericRecord user : records) {
        fileWriter.append(user);
      }
      fileWriter.close();
    }

    System.out.println("Avro file length :: " + new File(filePath.toString()).length() + " path " + filePath.toString());

    avroReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records2 = new ArrayList<>();
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(avroFile2, avroReader)) {
      fileReader.forEachRemaining(genericRecord -> records2.add(genericRecord));
        /*GenericRecord rec = fileReader.next();
        if (rec != null) {
          records.add(rec);
        } else {
          break;
        }
      }*/
  /*fileReader.close();
    }
    System.out.println("------ ");
    dataFileRec = (GenericRecord) records2.get(0).get("data_file");
    lowerBoundsArray = (GenericArray) dataFileRec.get("lower_bounds");
    lowerBoundsRec0 = (GenericData.Record) lowerBoundsArray.get(0);
    //CharBuffer lowerBound0 = DECODER.get().decode((ByteBuffer) lowerBoundsRec0.get("value"));
    lowerBoundsRec1 = (GenericData.Record) lowerBoundsArray.get(1);
    //CharBuffer lowerBound1 = DECODER.get().decode((ByteBuffer) lowerBoundsRec1.get("value"));
    lowerBoundsRec2 = (GenericData.Record) lowerBoundsArray.get(2);
    //CharBuffer lowerBound2 = DECODER.get().decode((ByteBuffer) lowerBoundsRec2.get("value"));
    lowerBoundsRec3 = (GenericData.Record) lowerBoundsArray.get(3);
    //CharBuffer lowerBound3 = DECODER.get().decode((ByteBuffer) lowerBoundsRec3.get("value"));
    lowerBoundsRec4 = (GenericData.Record) lowerBoundsArray.get(4);
    //CharBuffer lowerBound4 = DECODER.get().decode((ByteBuffer) lowerBoundsRec4.get("value"));
    System.out.println(",,,");
  }

  @Test
  public void testWriteReadHFileMetadataPayload1() throws Exception {
    String schemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataRecord\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"key\", // partition_col_filename\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"string\",\n" +
        "      \"avro.java.string\" : \"String\"\n" +
        "    }\n" +
        "  },    {   \"name\": \"col_stats\",\n" +
        "            \"doc\": \"Contains information about partitions and files within the dataset\",\n" +
        "            \"type\": [\"null\", {\n" +
        "               \"type\": \"map\",\n" +
        "               \"values\": {\n" +
        "                    \"type\": \"record\",\n" +
        "                    \"name\": \"HoodieMetadataFileInfo\",\n" +
        "                    \"fields\": [\n" +
        "                        {\n" +
        "                          \"name\" : \"mnv\",\n" +
        "                         \"type\" : \"bytes\",\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }, {\n" +
        "                          \"name\" : \"mxv\",\n" +
        "                          \"type\" : \"bytes\",\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }, {\n" +
        "                          \"name\" : \"sd\",\n" +
        "                          \"type\" : [ \"null\", \"long\" ],\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }, {\n" +
        "                          \"name\" : \"tv\",\n" +
        "                          \"type\" : [ \"null\", \"long\" ],\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }, {\n" +
        "                          \"name\" : \"nlv\",\n" +
        "                          \"type\" : [ \"null\", \"long\" ],\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }, {\n" +
        "                          \"name\" : \"nnv\",\n" +
        "                          \"type\" : [ \"null\", \"long\" ],\n" +
        "                          \"doc\" : \"\",\n" +
        "                          \"default\" : null\n" +
        "                        }\n" +
        "                    ]\n" +
        "                }\n" +
        "            }]\n" +
        "        }\n" +
        "  ]\n" +
        "}";

    String colRecordSchemaStr = "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"HoodieMetadataFileInfo\",\n" +
        "  \"namespace\" : \"org.apache.hudi.avro.model\",\n" +
        "  \"doc\" : \"A record saved within the Metadata Table\",\n" +
        "  \"fields\" : [\n" +
        "      {\n" +
        "          \"name\" : \"mnv\",\n" +
        "          \"type\" : \"bytes\",\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }, {\n" +
        "          \"name\" : \"mxv\",\n" +
        "          \"type\" : \"bytes\",\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }, {\n" +
        "          \"name\" : \"sd\",\n" +
        "          \"type\" : [ \"null\", \"long\" ],\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }, {\n" +
        "          \"name\" : \"tv\",\n" +
        "          \"type\" : [ \"null\", \"long\" ],\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }, {\n" +
        "          \"name\" : \"nlv\",\n" +
        "          \"type\" : [ \"null\", \"long\" ],\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }, {\n" +
        "          \"name\" : \"nnv\",\n" +
        "          \"type\" : [ \"null\", \"long\" ],\n" +
        "          \"doc\" : \"\",\n" +
        "          \"default\" : null\n" +
        "        }\n" +
        "  ]\n" +
        "}";

    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    Schema colRecSchema = new Schema.Parser().parse(colRecordSchemaStr);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new TreeMap<>();

    List<String> keys2 = new ArrayList<>();
    Map<String, GenericRecord> recordMap2 = new TreeMap<>();

    String stringIdPrefix = "col_str_";
    String longIdPrefix = "col_long_";
    String intIdPrefix = "col_int_";
    String doubleIdPrefix = "col_double_";
    String booleanIdPrefix = "col_bool_";

    int numPartitions = 1;
    int numCols = 100;
    int numFiles = 10000;
    int partitionIndex = 0;
    int colIndex = 0;
    int fileIndex = 0;

    for (int k = 0; k < numFiles; k++) {
      String fileId = UUID.randomUUID().toString();
      String writeToken = "23_04_235";
      String commitTime = "20211015191547";
      String fileName = fileId + "_" + writeToken + "_" + commitTime + ".parquet";
      String key = fileName;
      //System.out.println("file length " + fileName.length());
      HashMap<String, GenericRecord> colStats = new HashMap<>();
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("key", key);
      HashMap<String, GenericRecord> colStats2 = new HashMap<>();
      GenericRecord record2 = new GenericData.Record(avroSchema);
      String key2 = "" + fileIndex++;
      record2.put("key", key2);
      for (int j = 0; j < numCols; j++) {
        String colName = null;
        boolean isString = false;
        boolean isLong = false;
        boolean isInteger = false;
        boolean isDouble = false;
        boolean isBoolean = false;
        if (j % 5 == 0) {
          colName = stringIdPrefix + String.format("%04d", j);
          isString = true;
        } else if (j % 5 == 1) {
          colName = longIdPrefix + String.format("%04d", j);
          isLong = true;
        } else if (j % 5 == 2) {
          colName = intIdPrefix + String.format("%04d", j);
          isInteger = true;
        } else if (j % 5 == 3) {
          colName = doubleIdPrefix + String.format("%04d", j);
          isDouble = true;
        } else {
          colName = booleanIdPrefix + String.format("%04d", j);
          isBoolean = true;
        }
        ByteBuffer[] minMaxVals = getMinMaxValueByteBuffer(isString, isLong, isInteger, isDouble, isBoolean);
        byte[] minVal = minMaxVals[0].array();
        byte[] maxVal = minMaxVals[1].array();
        long sizeOnDisk = 10 + RANDOM.nextInt(1000);
        long numVals = 10 + RANDOM.nextInt(1000);
        long nullVals = 10 + RANDOM.nextInt(200);
        long nanVals = 10 + RANDOM.nextInt(300);
        GenericRecord colRecord = new GenericData.Record(colRecSchema);
        colRecord.put("mnv", ByteBuffer.wrap(minVal));
        colRecord.put("mxv", ByteBuffer.wrap(maxVal));
        colRecord.put("sd", sizeOnDisk);
        colRecord.put("tv", numVals);
        colRecord.put("nlv", nullVals);
        colRecord.put("nnv", nanVals);
        colStats.put(colName, colRecord);

        colStats2.put("" + (colIndex++), colRecord);
      }
      record.put("col_stats", colStats);
      recordMap.put(key, record);
      record2.put("col_stats", colStats2);
      recordMap2.put(key2, record2);
    }

    HoodieHFileWriter writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    long fileLength = new File(filePath.toString()).length();
    System.out.println("Total file size numPartitions : " + numPartitions + ",nulCols " + numCols + ", numFiles " + numFiles + " :: " + fileLength);

    new File(filePath.toString()).delete();

    writer = createHFileWriter(avroSchema);
    for (Map.Entry<String, GenericRecord> entry : recordMap2.entrySet()) {
      writer.writeAvro(entry.getKey(), entry.getValue());
    }
    writer.close();

    fileLength = new File(filePath.toString()).length();
    System.out.println("Encoded file length :: " + fileLength);

    String avroFileName = UUID.randomUUID().toString();
    String avroPath = filePath.getParent().toString() + "/" + avroFileName;
    File avroFile = new File(avroPath);

    // Serialize users to disk
    final DatumWriter<GenericRecord> avroWriter = new GenericDatumWriter<>(avroSchema);
    try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(avroWriter)) {
      fileWriter.setCodec(CodecFactory.deflateCodec(9));
      fileWriter.create(avroSchema, avroFile);
      for (GenericRecord user : recordMap2.values()) {
        fileWriter.append(user);
      }
      fileWriter.close();
    }
    System.out.println("Avro file length :: " + new File(avroPath).length());

  }*/

  public static ByteBuffer getKeyAsByteBuffer(int colId, long commitTime, int fileId) {
    return ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(colId).putLong(commitTime).putInt(fileId);
  }

  public static ByteBuffer getKeyAsByteBuffer(int colId, String partitionPath, long commitTime, int fileId) throws IOException {
    ByteBuffer partitionPathBytes = toByteBuffer(true, false, false, false, partitionPath);
    ByteBuffer firstPath = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(colId);
    ByteBuffer lastPart = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(commitTime).putInt(fileId);
    byte[] firstArray = firstPath.array();
    byte[] secondArray = partitionPathBytes.array();
    byte[] thirdArray = lastPart.array();

    ByteBuffer toReturn = ByteBuffer.allocate(firstArray.length + secondArray.length + thirdArray.length);
    return toReturn.put(firstArray, 0, firstArray.length).put(secondArray, 0, secondArray.length).put(thirdArray, 0, thirdArray.length);
  }

  public static ByteBuffer getKeyAsByteBuffer(long colId, long partitionId, long fileId) throws IOException {
    return ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN).putLong(colId).putLong(partitionId).putLong(fileId);
  }

  static <T> T[] concatWithStream(T[] array1, T[] array2) {
    return Stream.concat(Arrays.stream(array1), Arrays.stream(array2))
        .toArray(size -> (T[]) Array.newInstance(array1.getClass().getComponentType(), size));
  }

  public static ByteBuffer toByteBuffer(boolean isString, boolean isLong, boolean isInteger, boolean isDouble, Object value) throws IOException {
    if (value == null) {
      return null;
    }

    if (isString) {
      CharBuffer buffer = CharBuffer.wrap((CharSequence) value);
      try {
        return ENCODER.get().encode(buffer);
      } catch (CharacterCodingException e) {
        throw new IOException("Failed to encode value as UTF-8: " + e.getMessage());
      }
    } else if (isLong) {
      return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, (long) value);
    } else if (isInteger) {
      return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, (int) value);
    } else if (isDouble) {
      //double
      return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(0, (double) value);
    } else {
      return ByteBuffer.allocate(1).put(0, (Boolean) value ? (byte) 0x01 : (byte) 0x00);
    }
  }

  private String[] getMinMaxValue(boolean isString, boolean isLong, boolean isInteger, boolean isDouble, boolean isBoolean) {
    if (isString) {
      return new String[] {UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    } else if (isLong) {
      return new String[] {String.valueOf(RANDOM.nextLong()), String.valueOf(RANDOM.nextLong())};
    } else if (isInteger) {
      return new String[] {String.valueOf(RANDOM.nextInt()), String.valueOf(RANDOM.nextInt())};
    } else if (isDouble) {
      return new String[] {String.valueOf(RANDOM.nextDouble()), String.valueOf(RANDOM.nextDouble())};
    } else {
      return new String[] {String.valueOf(RANDOM.nextBoolean()), String.valueOf(RANDOM.nextBoolean())};
    }
  }

  private ByteBuffer[] getMinMaxValueByteBuffer(boolean isString, boolean isLong, boolean isInteger, boolean isDouble, boolean isBoolean) throws IOException {
    if (isString) {
      return new ByteBuffer[] {toByteBuffer(isString, isLong, isInteger, isDouble, generateRandomChars(16)),
          toByteBuffer(isString, isLong, isInteger, isDouble, generateRandomChars(16))};
    } else if (isLong) {
      return new ByteBuffer[] {toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextLong()),
          toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextLong())};
    } else if (isInteger) {
      return new ByteBuffer[] {toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextInt()),
          toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextInt())};
    } else if (isDouble) {
      return new ByteBuffer[] {toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextDouble()),
          toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextDouble())};
    } else {
      return new ByteBuffer[] {toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextBoolean()),
          toByteBuffer(isString, isLong, isInteger, isDouble, RANDOM.nextBoolean())};
    }
  }

  public static String generateRandomChars(int length) {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      sb.append(ALPHA_CHARS.charAt(random.nextInt(ALPHA_CHARS_LENGTH)));
    }
    return sb.toString();
  }

  private Set<String> getRandomKeys(int count, List<String> keys) {
    Set<String> rowKeys = new HashSet<>();
    int totalKeys = keys.size();
    while (rowKeys.size() < count) {
      int index = RANDOM.nextInt(totalKeys);
      if (!rowKeys.contains(index)) {
        rowKeys.add(keys.get(index));
      }
    }
    return rowKeys;
  }
}