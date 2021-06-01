package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class TestAvroPulsarSource extends UtilitiesTestBase {

  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String TOPIC_NAME = "test-topic";
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);
  private PulsarClient pulsarClient;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    pulsarClient = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    pulsarClient.close();
  }

  private TypedProperties createPropsForJsonSource(Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @Test
  public void testAvroPulsarSource() throws PulsarClientException {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
    recordSchemaBuilder.field("intField").type(SchemaType.INT32);
    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    Producer<org.apache.pulsar.client.api.schema.GenericRecord> producer = pulsarClient.newProducer(Schema.generic(schemaInfo))
        .topic(TOPIC_NAME).create();
    TypedProperties props = createPropsForJsonSource(null, "earliest");

    AvroPulsarSource avroPulsarSource = new AvroPulsarSource(props, jsc, sparkSession, null);
    SourceFormatAdapter pulsarSource = new SourceFormatAdapter(avroPulsarSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), pulsarSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());

    // GenericAvroSchema genericAvroSchema = (GenericAvroSchema) GenericSchemaImpl.of(schemaInfo);
    //new AvroRecordBuilderIm
    GenericSchema schema = Schema.generic(schemaInfo);
    producer.newMessage().value(schema.newRecordBuilder()
        .set("intField", 32)
        .build()).send();

    producer.newMessage().value(schema.newRecordBuilder()
        .set("intField", 50)
        .build()).send();

    producer.newMessage().value(schema.newRecordBuilder()
        .set("intField", 03)
        .build()).send();

    producer.newMessage().value(schema.newRecordBuilder()
        .set("intField", 10000)
        .build()).send();

    InputBatch<JavaRDD<GenericRecord>> fetch1 = pulsarSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(4, fetch1.getBatch().get().count());

  }
}
