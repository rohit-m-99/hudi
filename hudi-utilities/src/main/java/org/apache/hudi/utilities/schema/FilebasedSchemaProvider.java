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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  private static final Logger LOG = LogManager.getLogger(FilebasedSchemaProvider.class);

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.source.schema.file";
    private static final String TARGET_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.target.schema.file";
  }

  private final FileSystem fs;

  protected Schema sourceSchema;

  protected Schema targetSchema;

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE_PROP));
    String sourceFile = props.getString(Config.SOURCE_SCHEMA_FILE_PROP);
    this.fs = FSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    LOG.error("Calling parseSchema From CONSTRUCTOR - Applied Intuition");
    this.parseSchema();
  }

  private void parseSchema() {
    LOG.error("Inside parseSchema - Applied Intuition");
    String sourceFile = config.getString(Config.SOURCE_SCHEMA_FILE_PROP);
    LOG.error("Source Schema File: " + sourceFile + " - Applied Intuition");
    try {
      LOG.error("Source Schema: - Applied Intuition");
      this.sourceSchema = new Schema.Parser().parse(this.fs.open(new Path(sourceFile)));
      LOG.error(this.sourceSchema.toString(true));
      if (config.containsKey(Config.TARGET_SCHEMA_FILE_PROP)) {
        LOG.error("Target Schema: - Applied Intuition");
        this.targetSchema =
            new Schema.Parser().parse(fs.open(new Path(config.getString(Config.TARGET_SCHEMA_FILE_PROP))));
        LOG.error(this.targetSchema.toString(true));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Error reading schema", ioe);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }

  @Override
  public void refresh() {
    LOG.error("INSIDE FileBasedSchemaProvider's refresh() - Applied Intuition");
    parseSchema();
  }
}
