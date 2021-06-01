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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroPulsarSource extends AvroSource {

  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String TOPIC_NAME = "test-topic";
  private PulsarClient pulsarClient;
  private Consumer<org.apache.pulsar.client.api.schema.GenericRecord> pulsarConsumer;

  public AvroPulsarSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) throws PulsarClientException {
    super(props, sparkContext, sparkSession, schemaProvider);
    pulsarClient = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();
    pulsarConsumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
        .topic(TOPIC_NAME)
        .subscriptionType(SubscriptionType.Shared)
        .subscriptionName("testSubscription")
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .subscribe();
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
    try {
      if(lastCkptStr.isPresent()){
        pulsarConsumer.seek(MessageId.fromByteArrayWithTopic(lastCkptStr.get().getBytes(), TOPIC_NAME));
      }
      Messages<org.apache.pulsar.client.api.schema.GenericRecord> msgs = pulsarConsumer.batchReceive();
      List<GenericRecord> genRecs = new ArrayList<>();
      msgs.forEach(entry -> genRecs.add((GenericRecord) ((GenericAvroRecord)entry).getAvroRecord()));
      for(GenericRecord genericRecord: genRecs) {
        System.out.println("Deserialized Genrecs " + genericRecord.toString());
      }
      return new InputBatch(Option.of(sparkContext.parallelize(genRecs)), new String(pulsarConsumer.getLastMessageId().toByteArray()));
    } catch (PulsarClientException e) {
      return new InputBatch(Option.empty(), "");
    } catch (IOException e) {
      return new InputBatch(Option.empty(), "");
    }
  }
}
