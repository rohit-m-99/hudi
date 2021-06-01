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

package org.apache.hudi.client;

import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.SparkHoodieIndex;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.compact.SparkCompactHelpers;
import org.apache.hudi.table.upgrade.SparkUpgradeDowngrade;

import com.codahale.metrics.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings("checkstyle:LineLength")
public class SparkRDDWriteClient<Row> extends
    AbstractHoodieWriteClient<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkRDDWriteClient.class);

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending) {
    super(context, writeConfig, rollbackPending);
  }

  public SparkRDDWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig, boolean rollbackPending,
                             Option<EmbeddedTimelineService> timelineService) {
    super(context, writeConfig, rollbackPending, timelineService);
  }

  /**
   * Register hudi classes for Kryo serialization.
   *
   * @param conf instance of SparkConf
   * @return SparkConf
   */
  public static SparkConf registerClasses(SparkConf conf) {
    conf.registerKryoClasses(new Class[]{HoodieWriteConfig.class, HoodieRecord.class, HoodieKey.class});
    return conf;
  }

  @Override
  protected HoodieIndex<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> createIndex(HoodieWriteConfig writeConfig) {
    return SparkHoodieIndex.createIndex(config);
  }

  /**
   * Complete changes performed at the given instantTime marker with specified action.
   */
  @Override
  public boolean commit(String instantTime, Dataset<HoodieInternalWriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata,
                        String commitActionType, Map<String, List<String>> partitionToReplacedFileIds) {

    List<HoodieInternalWriteStatus> writeStatusList = writeStatuses.collectAsList();
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for(HoodieInternalWriteStatus internalWriteStatus: writeStatusList) {
      writeStats.add(internalWriteStatus.getStat());
    }
    //List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);
  }

  @Override
  protected HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> createTable(HoodieWriteConfig config,
                                                                                                           Configuration hadoopConf) {
    return HoodieSparkTable.create(config, context);
  }

  @Override
  public Dataset<Row> filterExists(Dataset<Row> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<Row> table = HoodieSparkTable.create(config, context);
    Timer.Context indexTimer = metrics.getIndexCtx();
    Dataset<Row> recordsWithLocation = getIndex().tagLocation(hoodieRecords, context, table);
    metrics.updateIndexMetrics(LOOKUP_STR, metrics.getDurationInMs(indexTimer == null ? 0L : indexTimer.stop()));
    // TODO
    return recordsWithLocation.filter(String.valueOf("date_time" != null));
    //return recordsWithLocation.filter(v1 -> !v1.isCurrentLocationKnown());
  }

  /**
   * Main API to run bootstrap to hudi.
   */
  @Override
  public void bootstrap(Option<Map<String, String>> extraMetadata) {
    if (rollbackPending) {
      rollBackInflightBootstrap();
    }
    getTableAndInitCtx(WriteOperationType.UPSERT, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS).bootstrap(context, extraMetadata);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> upsert(Dataset<Row> records, String instantTime) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT, instantTime);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.UPSERT);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.upsert(context, instantTime, records);
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(LOOKUP_STR, result.getIndexLookupDuration().get().toMillis());
    }
    return postWrite(result, instantTime, table);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> upsertPreppedRecords(Dataset<Row> preppedRecords, String instantTime) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.UPSERT_PREPPED, instantTime);
    table.validateUpsertSchema();
    setOperationType(WriteOperationType.UPSERT_PREPPED);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.upsertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> insert(Dataset<Row> records, String instantTime) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.insert(context,instantTime, records);
    return postWrite(result, instantTime, table);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> insertPreppedRecords(Dataset<Row> preppedRecords, String instantTime) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.INSERT_PREPPED, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT_PREPPED);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.insertPrepped(context,instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  /**
   * Removes all existing records from the partitions affected and inserts the given HoodieRecords, into the table.

   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwrite(Dataset<Row> records, final String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT_OVERWRITE);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata result = table.insertOverwrite(context, instantTime, records);
    return new HoodieWriteResult(postWrite(result, instantTime, table), result.getPartitionToReplaceFileIds());
  }


  /**
   * Removes all existing records of the Hoodie table and inserts the given HoodieRecords, into the table.

   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteResult insertOverwriteTable(Dataset<Row> records, final String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.INSERT_OVERWRITE_TABLE, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.INSERT_OVERWRITE_TABLE);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata result = table.insertOverwriteTable(context, instantTime, records);
    return new HoodieWriteResult(postWrite(result, instantTime, table), result.getPartitionToReplaceFileIds());
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> bulkInsert(Dataset<Row> records, String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> bulkInsert(Dataset<Row> records, String instantTime, Option<BulkInsertPartitioner<Dataset<Row>>> userDefinedBulkInsertPartitioner) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.BULK_INSERT, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.bulkInsert(context,instantTime, records, userDefinedBulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> bulkInsertPreppedRecords(Dataset<Row> preppedRecords, String instantTime, Option<BulkInsertPartitioner<Dataset<Row>>> bulkInsertPartitioner) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table =
        getTableAndInitCtx(WriteOperationType.BULK_INSERT_PREPPED, instantTime);
    table.validateInsertSchema();
    setOperationType(WriteOperationType.BULK_INSERT_PREPPED);
    this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this, instantTime);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.bulkInsertPrepped(context,instantTime, preppedRecords, bulkInsertPartitioner);
    return postWrite(result, instantTime, table);
  }

  @Override
  public Dataset<HoodieInternalWriteStatus> delete(Dataset<Row> keys, String instantTime) {
    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table = getTableAndInitCtx(WriteOperationType.DELETE, instantTime);
    setOperationType(WriteOperationType.DELETE);
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result = table.delete(context,instantTime, keys);
    return postWrite(result, instantTime, table);
  }

  @Override
  protected Dataset<HoodieInternalWriteStatus> postWrite(HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> result,
                                           String instantTime,
                                           HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> hoodieTable) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(hoodieTable, result.getCommitMetadata().get(), instantTime, Option.empty());

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(), hoodieTable.getMetaClient().getCommitActionType());
    }
    return result.getWriteStatuses();
  }

  @Override
  public void commitCompaction(String compactionInstantTime, Dataset<HoodieInternalWriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieSparkTable<Row> table = HoodieSparkTable.create(config, context);
    HoodieCommitMetadata metadata = SparkCompactHelpers.newInstance().createCompactionMetadata(
        table, compactionInstantTime, writeStatuses, config.getSchema());
    extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
    completeCompaction(metadata, writeStatuses, table, compactionInstantTime);
  }

  @Override
  protected void completeCompaction(HoodieCommitMetadata metadata, Dataset<HoodieInternalWriteStatus> writeStatuses,
                                    HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> table,
                                    String compactionCommitTime) {
    List<HoodieInternalWriteStatus> writeStatusList = writeStatuses.collectAsList();
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    for(HoodieInternalWriteStatus writeStatus: writeStatusList){
      writeStats.add(writeStatus.getStat());
    }
    //List<HoodieWriteStat> writeStats = writeStatuses.map(WriteStatus::getStat).collect();
    finalizeWrite(table, compactionCommitTime, writeStats);
    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    SparkCompactHelpers.newInstance().completeInflightCompaction(table, compactionCommitTime, metadata);

    if (compactionTimer != null) {
      long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
      try {
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
            durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
      } catch (ParseException e) {
        throw new HoodieCommitException("Commit time is not of valid format. Failed to commit compaction "
            + config.getBasePath() + " at time " + compactionCommitTime, e);
      }
    }
    LOG.info("Compacted successfully on commit " + compactionCommitTime);
  }

  @Override
  protected Dataset<HoodieInternalWriteStatus> compact(String compactionInstantTime, boolean shouldComplete) {
    HoodieSparkTable<Row> table = HoodieSparkTable.create(config, context);
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      rollbackInflightCompaction(inflightInstant, table);
      table.getMetaClient().reloadActiveTimeline();
    }
    compactionTimer = metrics.getCompactionCtx();
    HoodieWriteMetadata<Dataset<HoodieInternalWriteStatus>> compactionMetadata = table.compact(context, compactionInstantTime);
    Dataset<HoodieInternalWriteStatus> statuses = compactionMetadata.getWriteStatuses();
    if (shouldComplete && compactionMetadata.getCommitMetadata().isPresent()) {
      completeCompaction(compactionMetadata.getCommitMetadata().get(), statuses, table, compactionInstantTime);
    }
    return statuses;
  }

  @Override
  protected HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> getTableAndInitCtx(WriteOperationType operationType, String instantTime) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    new SparkUpgradeDowngrade(metaClient, config, context).run(metaClient, HoodieTableVersion.current(), config, context, instantTime);
    return getTableAndInitCtx(metaClient, operationType);
  }

  private HoodieTable<Row, Dataset<Row>, Dataset<Row>, Dataset<HoodieInternalWriteStatus>> getTableAndInitCtx(HoodieTableMetaClient metaClient, WriteOperationType operationType) {
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieSparkTable<Row> table = HoodieSparkTable.create(config, (HoodieSparkEngineContext) context, metaClient);
    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
      writeTimer = metrics.getCommitCtx();
    } else {
      writeTimer = metrics.getDeltaCommitCtx();
    }
    return table;
  }
}
