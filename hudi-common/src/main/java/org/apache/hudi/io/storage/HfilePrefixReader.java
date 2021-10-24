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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HfilePrefixReader {

  private static final Logger LOG = LogManager.getLogger(HfilePrefixReader.class);

  public static void main(String[] args) throws IOException {
    String fileLocationToRead = args[0];
    int numKeysToRead = Integer.parseInt(args[1]);
    int numKeyChars = Integer.parseInt(args[2]);
    prefixSearch(fileLocationToRead, numKeysToRead, numKeyChars);
  }

  public static void prefixSearch(String fileLocationToRead, int numKeysToRead, int numKeyChars) throws IOException {

    int numCols = 500;
    int numFiles = 100;
    long commitTime = 20211015191547000L;
    List<String> keys = new ArrayList<>();
    for (int j = 0; j < numCols; j++) {
      for (int k = 0; k < numFiles; k++) {
        keys.add((Integer.toString(j) + commitTime + Integer.toString(k)));
      }
    }

    Collections.shuffle(keys);
    List<String> keysToSearch = new ArrayList<>();
    int count = 0;
    while (count < numKeysToRead) {
      String candidateKey = keys.get(0);
      String candidateKeyPrefix = candidateKey.substring(0, numKeyChars);
      if (!keysToSearch.contains(candidateKeyPrefix)) {
        keysToSearch.add(candidateKeyPrefix);
        count++;
      }
    }

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);

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
    HoodieHFileReader hfileReader = new HoodieHFileReader(conf, new Path(fileLocationToRead), cacheConfig);
    long startTimeMs = System.currentTimeMillis();
    List<Pair<String, GenericRecord>> result = hfileReader.getRecordsByKeyPrefix("195", avroSchema);
    LOG.warn("Prefix look up of one key took " + (System.currentTimeMillis() - startTimeMs) + "ms. Total matched entries " + result.size());
  }
}
