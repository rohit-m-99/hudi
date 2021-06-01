package org.apache.hudi.io.storage;

import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieParquetWriter extends HoodieClientTestHarness {

  private static final Random RANDOM = new Random();

  MapType mapType = DataTypes.createMapType(StringType, new StructType().add("mapKey", "string", false).add("mapVal", "integer", true));
  ArrayType arrayType =  new ArrayType(new StructType().add("arrayKey", "string", false).add("arrayVal", "integer", true), true);
  ArrayType intArray = new ArrayType(DataTypes.IntegerType, true);
  StructType innerStruct = new StructType().add("innerKey","string",false).add("value", "long", true);

  StructType struct = new StructType().add("key", "string", false).add("version", "string", true)
      //.add("data1",innerStruct,false).add("data2",innerStruct,true)
      //.add("nullableMap", mapType, true).add("map",mapType,false)
      .add("nullableArray", intArray, true);
      //.add("array",arrayType,false);

  private Schema schema = AvroConversionUtils.convertStructTypeToAvroSchema(struct, "test.struct.name", "test.struct.name.space");
  private MessageType parquetSchema = new AvroSchemaConverter().convert(schema);

  private StructType sparkDataType = AvroConversionUtils.convertAvroSchemaToStructType(AVRO_SCHEMA);

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieInternalRowParquetWriter");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void endToEndTest() throws Exception {
    HoodieWriteConfig cfg = SparkDatasetTestUtils.getConfigBuilder(basePath).build();
    GenericRecordFullPayloadGenerator payloadGenerator = new GenericRecordFullPayloadGenerator(schema, 1024 * 100);

    for (int i = 0; i < 1; i++) {
      // init write support and parquet config
      HoodieAvroWriteSupport writeSupport = getWriteSupport(cfg);
      /*HoodieRowParquetConfig parquetConfig = new HoodieRowParquetConfig(writeSupport,
          CompressionCodecName.SNAPPY, cfg.getParquetBlockSize(), cfg.getParquetPageSize(), cfg.getParquetMaxFileSize(),
          writeSupport.getHadoopConf(), cfg.getParquetCompressionRatio());*/
      hadoopConf.set("parquet.avro.write-old-list-structure","false");
      // hadoopConf.set("spark.sql.parquet.mergeSchema","false");
      // hadoopConf.set("spark.sql.parquet.filterPushdown","true");
      HoodieAvroParquetConfig avroParquetConfig = new HoodieAvroParquetConfig(writeSupport, CompressionCodecName.SNAPPY, cfg.getParquetBlockSize(), cfg.getParquetPageSize(), cfg.getParquetMaxFileSize(),
          hadoopConf, cfg.getParquetCompressionRatio());
      // prepare path
      String fileId = UUID.randomUUID().toString();
      Path filePath = new Path(basePath + "/" + fileId);
      String partitionPath = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
      metaClient.getFs().mkdirs(new Path(basePath));

      // init writer
      HoodieParquetWriter hoodieParquetWriter = new HoodieParquetWriter<>("000", filePath, avroParquetConfig, null, context.getTaskContextSupplier());

      // generate input
      int size = 5 ;
      List<GenericRecord> genericRecords = new ArrayList<>();
      for(int j =0;j< size;j++) {
        genericRecords.add(payloadGenerator.getNewPayload());
      }
      // Generate inputs
      /*Dataset<Row> inputRows = SparkDatasetTestUtils.getRandomRows(sqlContext, size, partitionPath, false);
      List<InternalRow> internalRows = SparkDatasetTestUtils.toInternalRows(inputRows, SparkDatasetTestUtils.ENCODER); */

      // issue writes
      for (GenericRecord genericRecord : genericRecords) {
        hoodieParquetWriter.writeAvro(genericRecord.get("key").toString(), genericRecord);
      }

      // close the writer
      hoodieParquetWriter.close();

      // verify rows
      Dataset<Row> result = sqlContext.read().parquet(basePath);

      System.out.println("Schema for gen Rec " + genericRecords.get(0).getSchema().toString());
      System.out.println("Schema for Dataset<Row> : " + result.schema().toString());


      genericRecords.forEach(entry -> System.out.println("Gen rec: " + entry.toString()));
      System.out.println("----------------------------------------------------------------");
      result.collectAsList().forEach(entry -> System.out.println("Row:     " + entry.toString()));
    }
  }

  private GenericRecord getRandomRecord(long uniqueIdentifier) {
    GenericRecord rec = new GenericData.Record(schema);
    rec.put("key", "key_" + uniqueIdentifier);
    rec.put("version", Long.toString(System.currentTimeMillis()));
    return rec;
  }

  private HoodieAvroWriteSupport getWriteSupport(HoodieWriteConfig writeConfig) {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
        writeConfig.getBloomFilterNumEntries(),
        writeConfig.getBloomFilterFPP(),
        writeConfig.getDynamicBloomFilterMaxNumEntries(),
        writeConfig.getBloomFilterType());
    return new HoodieAvroWriteSupport(parquetSchema, schema, filter);
  }

}
