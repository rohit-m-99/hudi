/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.async.AsyncClusteringService;
import org.apache.hudi.async.AsyncCompactService;
import org.apache.hudi.async.HoodieAsyncService;
import org.apache.hudi.async.SparkAsyncCompactService;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallback;
import org.apache.hudi.utilities.callback.kafka.HoodieWriteCommitKafkaCallbackConfig;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallback;
import org.apache.hudi.utilities.callback.pulsar.HoodieWriteCommitPulsarCallbackConfig;
import org.apache.hudi.utilities.deltastreamer.PostWriteTerminationStrategy;
import org.apache.hudi.utilities.deltastreamer.SchedulerConfGenerator;
import org.apache.hudi.utilities.deltastreamer.TerminationStrategyUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE;
import static org.apache.hudi.config.HoodieClusteringConfig.INLINE_CLUSTERING;
import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT;
import static org.apache.hudi.config.HoodieWriteConfig.AUTO_COMMIT_ENABLE;
import static org.apache.hudi.config.HoodieWriteConfig.COMBINE_BEFORE_UPSERT;

public class CompactTableService implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(CompactTableService.class);

  protected final transient Config cfg;

  /**
   * NOTE: These properties are already consolidated w/ CLI provided config-overrides.
   */
  private final TypedProperties properties;

  protected transient Option<CompactPollingService> compactPollingService;


  public static final String DELTASYNC_POOL_NAME = "hoodiedeltasync";

  public CompactTableService(Config cfg, JavaSparkContext jssc) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), Option.empty());
  }

  public CompactTableService(Config cfg, JavaSparkContext jssc, Option<TypedProperties> props) throws IOException {
    this(cfg, jssc, FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration()),
        jssc.hadoopConfiguration(), props);
  }

  public CompactTableService(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf) throws IOException {
    this(cfg, jssc, fs, conf, Option.empty());
  }

  public CompactTableService(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                             Option<TypedProperties> propsOverride) throws IOException {
    this.properties = combineProperties(cfg, propsOverride, jssc.hadoopConfiguration());
    this.cfg = cfg;
    this.compactPollingService = Option.ofNullable(new CompactPollingService(cfg, jssc, fs, conf, Option.ofNullable(this.properties)));
  }

  private static TypedProperties combineProperties(Config cfg, Option<TypedProperties> propsOverride, Configuration hadoopConf) {
    HoodieConfig hoodieConfig = new HoodieConfig();
    // Resolving the properties in a consistent way:
    //   1. Properties override always takes precedence
    //   2. Otherwise, check if there's no props file specified (merging in CLI overrides)
    //   3. Otherwise, parse provided specified props file (merging in CLI overrides)
    if (propsOverride.isPresent()) {
      hoodieConfig.setAll(propsOverride.get());
    } else if (cfg.propsFilePath.equals(Config.DEFAULT_DFS_SOURCE_PROPERTIES)) {
      hoodieConfig.setAll(UtilHelpers.getConfig(cfg.configs).getProps());
    } else {
      hoodieConfig.setAll(UtilHelpers.readConfig(hadoopConf, new Path(cfg.propsFilePath), cfg.configs).getProps());
    }

    // set any configs that Deltastreamer has to override explicitly
    hoodieConfig.setDefaultValue(DataSourceWriteOptions.RECONCILE_SCHEMA());
    // we need auto adjustment enabled for deltastreamer since async table services are feasible within the same JVM.
    hoodieConfig.setValue(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(), "true");
    if (cfg.tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      // Explicitly set the table type
      hoodieConfig.setValue(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    }

    return hoodieConfig.getProps(true);
  }

  public void shutdownGracefully() {
    compactPollingService.ifPresent(ds -> ds.shutdown(false));
  }

  /**
   * Main method to start syncing.
   *
   * @throws Exception
   */
  public void sync() throws Exception {
    if (cfg.continuousMode) {
      compactPollingService.ifPresent(ds -> {
        ds.start(this::onDeltaSyncShutdown);
        try {
          ds.waitForShutdown();
        } catch (Exception e) {
          throw new HoodieException(e.getMessage(), e);
        }
      });
      LOG.info("Compact Polling service shutting down");
    }
    //    } else {
    //      LOG.info("Delta Streamer running only single round");
    //      try {
    //        deltaSyncService.ifPresent(ds -> {
    //          try {
    //            ds.getDeltaSync().syncOnce();
    //          } catch (IOException e) {
    //            throw new HoodieIOException(e.getMessage(), e);
    //          }
    //        });
    //      } catch (Exception ex) {
    //        LOG.error("Got error running delta sync once. Shutting down", ex);
    //        throw ex;
    //      } finally {
    //        deltaSyncService.ifPresent(HoodieDeltaStreamer.DeltaSyncService::close);
    //        Metrics.shutdown();
    //        LOG.info("Shut down delta streamer");
    //      }
    //    }
  }

  public Config getConfig() {
    return cfg;
  }

  private boolean onDeltaSyncShutdown(boolean error) {
    LOG.info("DeltaSync shutdown. Closing write client. Error?" + error);
    compactPollingService.ifPresent(CompactPollingService::close);
    return true;
  }

  public static class Config implements Serializable {
    public static final String DEFAULT_DFS_SOURCE_PROPERTIES = "file://" + System.getProperty("user.dir")
        + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--target-base-path"},
        description = "base path for the target hoodie table. "
            + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table)",
        required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {"--target-table"}, description = "name of the target table", required = true)
    public String targetTableName;

    @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String tableType;

    @Parameter(names = {"--base-file-format"}, description = "File format for the base files. PARQUET (or) HFILE", required = false)
    public String baseFileFormat = "PARQUET";

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties."
        + " Properties in this file can be overridden by \"--hoodie-conf\"")
    public String propsFilePath = DEFAULT_DFS_SOURCE_PROPERTIES;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
            + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--max-pending-clustering"},
        description = "Maximum number of outstanding inflight/requested clustering. Delta Sync will not happen unless"
            + "outstanding clustering is less than this number")
    public Integer maxPendingClustering = 5;

    @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = false;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;

    @Parameter(names = {"--spark-master"},
        description = "spark master to use, if not defined inherits from your environment taking into "
            + "account Spark Configuration priority rules (e.g. not using spark-submit command).")
    public String sparkMaster = "";

    @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
    public Boolean commitOnErrors = false;

    @Parameter(names = {"--delta-sync-scheduling-weight"},
        description = "Scheduling weight for delta sync as defined in "
            + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingWeight = 1;

    @Parameter(names = {"--compact-scheduling-weight"}, description = "Scheduling weight for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingWeight = 1;

    @Parameter(names = {"--delta-sync-scheduling-minshare"}, description = "Minshare for delta sync as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer deltaSyncSchedulingMinShare = 0;

    @Parameter(names = {"--compact-scheduling-minshare"}, description = "Minshare for compaction as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer compactSchedulingMinShare = 0;

    /**
     * Compaction is enabled for MoR table by default. This flag disables it
     */
    @Parameter(names = {"--disable-compaction"},
        description = "Compaction is enabled for MoR table by default. This flag disables it ")
    public Boolean forceDisableCompaction = false;

    @Parameter(names = {"--retry-on-source-failures"}, description = "Retry on any source failures")
    public Boolean retryOnSourceFailures = false;

    @Parameter(names = {"--retry-interval-seconds"}, description = "the retry interval for source failures if --retry-on-source-failures is enabled")
    public Integer retryIntervalSecs = 30;

    @Parameter(names = {"--max-retry-count"}, description = "the max retry count if --retry-on-source-failures is enabled")
    public Integer maxRetryCount = 3;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--retry-last-pending-inline-clustering", "-rc"}, description = "Retry last pending inline clustering plan before writing to sink.")
    public Boolean retryLastPendingInlineClusteringJob = false;

    @Parameter(names = {"--cluster-scheduling-weight"}, description = "Scheduling weight for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingWeight = 1;

    @Parameter(names = {"--cluster-scheduling-minshare"}, description = "Minshare for clustering as defined in "
        + "https://spark.apache.org/docs/latest/job-scheduling.html")
    public Integer clusterSchedulingMinShare = 0;

    @Parameter(names = {"--post-write-termination-strategy-class"}, description = "Post writer termination strategy class to gracefully shutdown deltastreamer in continuous mode")
    public String postWriteTerminationStrategyClass = "";

    public boolean isAsyncCompactionEnabled() {
      return continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    public boolean isInlineCompactionEnabled() {
      // Inline compaction is disabled for continuous mode, otherwise enabled for MOR
      return !continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Config config = (Config) o;
      return Objects.equals(targetBasePath, config.targetBasePath) && Objects.equals(targetTableName, config.targetTableName)
          && Objects.equals(tableType, config.tableType) && Objects.equals(baseFileFormat, config.baseFileFormat) && Objects.equals(propsFilePath, config.propsFilePath)
          && Objects.equals(configs, config.configs) && Objects.equals(maxPendingCompactions, config.maxPendingCompactions)
          && Objects.equals(maxPendingClustering, config.maxPendingClustering) && Objects.equals(continuousMode, config.continuousMode)
          && Objects.equals(minSyncIntervalSeconds, config.minSyncIntervalSeconds) && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(commitOnErrors, config.commitOnErrors) && Objects.equals(deltaSyncSchedulingWeight, config.deltaSyncSchedulingWeight)
          && Objects.equals(compactSchedulingWeight, config.compactSchedulingWeight) && Objects.equals(deltaSyncSchedulingMinShare, config.deltaSyncSchedulingMinShare)
          && Objects.equals(compactSchedulingMinShare, config.compactSchedulingMinShare) && Objects.equals(forceDisableCompaction, config.forceDisableCompaction)
          && Objects.equals(retryOnSourceFailures, config.retryOnSourceFailures) && Objects.equals(retryIntervalSecs, config.retryIntervalSecs)
          && Objects.equals(maxRetryCount, config.maxRetryCount) && Objects.equals(help, config.help)
          && Objects.equals(retryLastPendingInlineClusteringJob, config.retryLastPendingInlineClusteringJob) && Objects.equals(clusterSchedulingWeight, config.clusterSchedulingWeight)
          && Objects.equals(clusterSchedulingMinShare, config.clusterSchedulingMinShare) && Objects.equals(postWriteTerminationStrategyClass, config.postWriteTerminationStrategyClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(targetBasePath, targetTableName, tableType, baseFileFormat, propsFilePath, configs, maxPendingCompactions, maxPendingClustering, continuousMode, minSyncIntervalSeconds,
          sparkMaster, commitOnErrors, deltaSyncSchedulingWeight, compactSchedulingWeight, deltaSyncSchedulingMinShare, compactSchedulingMinShare, forceDisableCompaction, retryOnSourceFailures,
          retryIntervalSecs, maxRetryCount, retryLastPendingInlineClusteringJob, clusterSchedulingWeight, clusterSchedulingMinShare, postWriteTerminationStrategyClass);
    }

    @Override
    public String toString() {
      return "Config{"
          + "targetBasePath='" + targetBasePath + '\''
          + ", targetTableName='" + targetTableName + '\''
          + ", tableType='" + tableType + '\''
          + ", baseFileFormat='" + baseFileFormat + '\''
          + ", propsFilePath='" + propsFilePath + '\''
          + ", configs=" + configs
          + ", maxPendingCompactions=" + maxPendingCompactions
          + ", maxPendingClustering=" + maxPendingClustering
          + ", continuousMode=" + continuousMode
          + ", minSyncIntervalSeconds=" + minSyncIntervalSeconds
          + ", sparkMaster='" + sparkMaster + '\''
          + ", commitOnErrors=" + commitOnErrors
          + ", deltaSyncSchedulingWeight=" + deltaSyncSchedulingWeight
          + ", compactSchedulingWeight=" + compactSchedulingWeight
          + ", deltaSyncSchedulingMinShare=" + deltaSyncSchedulingMinShare
          + ", compactSchedulingMinShare=" + compactSchedulingMinShare
          + ", forceDisableCompaction=" + forceDisableCompaction
          + ", retryOnSourceFailures=" + retryOnSourceFailures
          + ", retryIntervalSecs=" + retryIntervalSecs
          + ", maxRetryCount=" + maxRetryCount
          + ", retryLastPendingInlineClusteringJob=" + retryLastPendingInlineClusteringJob
          + ", clusterSchedulingWeight=" + clusterSchedulingWeight
          + ", clusterSchedulingMinShare=" + clusterSchedulingMinShare
          + ", postWriteTerminationStrategyClass='" + postWriteTerminationStrategyClass + '\''
          + '}';
    }
  }

  private static String toSortedTruncatedString(TypedProperties props) {
    List<String> allKeys = new ArrayList<>();
    for (Object k : props.keySet()) {
      allKeys.add(k.toString());
    }
    Collections.sort(allKeys);
    StringBuilder propsLog = new StringBuilder("Creating delta streamer with configs:\n");
    for (String key : allKeys) {
      String value = Option.ofNullable(props.get(key)).orElse("").toString();
      // Truncate too long values.
      if (value.length() > 255 && !LOG.isDebugEnabled()) {
        value = value.substring(0, 128) + "[...]";
      }
      propsLog.append(key).append(": ").append(value).append("\n");
    }
    return propsLog.toString();
  }

  public static final Config getConfig(String[] args) {
    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    return cfg;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = getConfig(args);
    Map<String, String> additionalSparkConfigs = SchedulerConfGenerator.getSparkSchedulingConfigs(cfg);
    JavaSparkContext jssc = null;
    if (StringUtils.isNullOrEmpty(cfg.sparkMaster)) {
      jssc = UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, additionalSparkConfigs);
    } else {
      jssc = UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster, additionalSparkConfigs);
    }

    try {
      new CompactTableService(cfg, jssc).sync();
    } finally {
      jssc.stop();
    }
  }


  /**
   * Syncs data either in single-run or in continuous mode.
   */
  public static class CompactPollingService extends HoodieAsyncService {

    private static final long serialVersionUID = 1L;
    /**
     * Delta Sync Config.
     */
    private final Config cfg;

    /**
     * Spark Session.
     */
    private transient SparkSession sparkSession;

    /**
     * Spark context.
     */
    private transient JavaSparkContext jssc;

    /**
     * Bag of properties with source, hoodie client, key generator etc.
     */
    TypedProperties props;

    /**
     * Async Compactor Service.
     */
    private Map<String, Option<AsyncCompactService>> asyncCompactServices = new HashMap<>();

    /**
     * Async clustering service.
     */
    private Map<String, Option<AsyncClusteringService>> asyncClusteringServices = new HashMap<>();

    /**
     * Table Type.
     */
    private Map<String, HoodieTableType> tableTypes = new HashMap<>();

    /**
     * Delta Sync.
     */
    private transient Map<String, SparkRDDWriteClient> writeClients = new HashMap<>();

    /**
     * DeltaSync will explicitly manage embedded timeline server so that they can be reused across Write Client
     * instantiations.
     */
    private transient Option<EmbeddedTimelineService> embeddedTimelineService = Option.empty();

    private Map<String, HoodieTableMetaClient> metaClients = new HashMap<>();

    private final Option<PostWriteTerminationStrategy> postWriteTerminationStrategy;

    private HoodieInstant lastScheduledCompactInstant;

    public CompactPollingService(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf,
                                 Option<TypedProperties> properties) throws IOException {
      this.cfg = cfg;
      this.jssc = jssc;
      this.sparkSession = SparkSession.builder().config(jssc.getConf()).getOrCreate();
      this.asyncCompactServices = new HashMap<>();
      this.asyncClusteringServices = new HashMap<>();
      this.postWriteTerminationStrategy = StringUtils.isNullOrEmpty(cfg.postWriteTerminationStrategyClass) ? Option.empty() :
          TerminationStrategyUtils.createPostWriteTerminationStrategy(properties.get(), cfg.postWriteTerminationStrategyClass);

      String[] targetPaths = cfg.targetBasePath.split(",");
      String[] targetTables = cfg.targetTableName.split(",");
      int targetTableIndex = 0;
      for (String targetPath : targetPaths) {
        LOG.info("Processing table :: " + targetPath);
        if (fs.exists(new Path(targetPath))) {
          HoodieTableMetaClient meta =
              HoodieTableMetaClient.builder().setConf(new Configuration(fs.getConf())).setBasePath(targetPath).setLoadActiveTimelineOnLoad(true).build();
          tableTypes.put(targetPath, meta.getTableType());
          String baseFileFormat = meta.getTableConfig().getBaseFileFormat().toString();
          cfg.baseFileFormat = baseFileFormat;
        } else {
          throw new HoodieIOException("Table does not exist " + targetPath);
        }

        this.props = properties.get();
        LOG.info(toSortedTruncatedString(props));

        HoodieWriteConfig hoodieCfg = getHoodieClientConfig(jssc, targetPath, targetTables[targetTableIndex]);
        if (targetTableIndex == 0) {
          LOG.warn("Starting Embedded timeline server ");
          embeddedTimelineService = EmbeddedTimelineServerHelper.createEmbeddedTimelineService(new HoodieSparkEngineContext(jssc), hoodieCfg);
        } else {
          LOG.warn("Re-using same embedded timeline server");
          hoodieCfg = EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), hoodieCfg);
        }
        LOG.warn("Creating new write client ");
        writeClients.put(targetPath, new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jssc), hoodieCfg, embeddedTimelineService));
        initializeAsyncTableServices(writeClients.get(targetPath), targetPath);
        targetTableIndex++;
      }
    }

    private HoodieWriteConfig getHoodieClientConfig(JavaSparkContext jssc, String targetPath, String targetTable) {
      final boolean combineBeforeUpsert = true;
      final boolean autoCommit = false;

      // NOTE: Provided that we're injecting combined properties
      //       (from {@code props}, including CLI overrides), there's no
      //       need to explicitly set up some configuration aspects that
      //       are based on these (for ex Clustering configuration)
      HoodieWriteConfig.Builder builder =
          HoodieWriteConfig.newBuilder()
              .withPath(targetPath)
              .withCompactionConfig(
                  HoodieCompactionConfig.newBuilder()
                      .withInlineCompaction(cfg.isInlineCompactionEnabled())
                      .build()
              )
              .withPayloadConfig(
                  HoodiePayloadConfig.newBuilder()
                      .withPayloadClass(cfg.payloadClassName)
                      .withPayloadOrderingField(cfg.sourceOrderingField)
                      .build())
              .forTable(targetTable)
              .withAutoCommit(autoCommit)
              .withProps(props);

      this.metaClients.put(targetPath, HoodieTableMetaClient.builder().setConf(new Configuration(jssc.hadoopConfiguration())).setBasePath(targetPath).build());
      Schema newWriteSchema = null;
      HoodieTableMetaClient metaClient = metaClients.get(targetPath);
      int totalCompleted = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants();
      if (totalCompleted > 0) {
        try {
          TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
          newWriteSchema = schemaResolver.getTableAvroSchema(false);
        } catch (IllegalArgumentException e) {
          LOG.warn("Could not fetch schema from table. Falling back to using target schema from schema provider");
        } catch (Exception e) {
          LOG.error("Failed to parse schema from commit metadata ", e);
          throw new HoodieException("Failed to parse schema from commit metadata ", e);
        }
      }

      if (newWriteSchema != null) {
        builder.withSchema(newWriteSchema.toString());
      }

      HoodieWriteConfig config = builder.build();

      if (config.writeCommitCallbackOn()) {
        // set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
        if (HoodieWriteCommitKafkaCallback.class.getName().equals(config.getCallbackClass())) {
          HoodieWriteCommitKafkaCallbackConfig.setCallbackKafkaConfigIfNeeded(config);
        }

        // set default value for {@link HoodieWriteCommitPulsarCallbackConfig} if needed.
        if (HoodieWriteCommitPulsarCallback.class.getName().equals(config.getCallbackClass())) {
          HoodieWriteCommitPulsarCallbackConfig.setCallbackPulsarConfigIfNeeded(config);
        }
      }

      HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(props);

      // Validate what deltastreamer assumes of write-config to be really safe
      ValidationUtils.checkArgument(config.inlineCompactionEnabled() == cfg.isInlineCompactionEnabled(),
          String.format("%s should be set to %s", INLINE_COMPACT.key(), cfg.isInlineCompactionEnabled()));
      ValidationUtils.checkArgument(config.inlineClusteringEnabled() == clusteringConfig.isInlineClusteringEnabled(),
          String.format("%s should be set to %s", INLINE_CLUSTERING.key(), clusteringConfig.isInlineClusteringEnabled()));
      ValidationUtils.checkArgument(config.isAsyncClusteringEnabled() == clusteringConfig.isAsyncClusteringEnabled(),
          String.format("%s should be set to %s", ASYNC_CLUSTERING_ENABLE.key(), clusteringConfig.isAsyncClusteringEnabled()));
      ValidationUtils.checkArgument(!config.shouldAutoCommit(),
          String.format("%s should be set to %s", AUTO_COMMIT_ENABLE.key(), autoCommit));
      ValidationUtils.checkArgument(config.shouldCombineBeforeUpsert(),
          String.format("%s should be set to %s", COMBINE_BEFORE_UPSERT.key(), combineBeforeUpsert));
      return config;
    }

    public CompactPollingService(Config cfg, JavaSparkContext jssc, FileSystem fs, Configuration conf)
        throws IOException {
      this(cfg, jssc, fs, conf, Option.empty());
    }

    @Override
    protected Pair<CompletableFuture, ExecutorService> startService() {
      ExecutorService executor = Executors.newFixedThreadPool(1);
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        boolean error = false;
        if (cfg.isAsyncCompactionEnabled()) {
          // set Scheduler Pool.
          LOG.info("Setting Spark Pool name for delta-sync to " + DELTASYNC_POOL_NAME);
          jssc.setLocalProperty("spark.scheduler.pool", DELTASYNC_POOL_NAME);
        }

        HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.from(props);

        try {
          while (!isShutdownRequested()) {
            try {
              long start = System.currentTimeMillis();
              for (Map.Entry<String, Option<AsyncCompactService>> entry : asyncCompactServices.entrySet()) {
                String targetPath = entry.getKey();
                Option<AsyncCompactService> asyncCompactServiceOption = entry.getValue();
                List<HoodieInstant> inflightCompactionInstants = metaClients.get(targetPath).reloadActiveTimeline()
                    .filterPendingCompactionTimeline().getInstants().collect(Collectors.toList());
                if (!inflightCompactionInstants.isEmpty()) {
                  int index = 0;
                  HoodieInstant compactInstant = inflightCompactionInstants.get(index);
                  LOG.info("Processing instant " + compactInstant + ", while last scheduled compaction was " + lastScheduledCompactInstant != null ? lastScheduledCompactInstant : "null");
                  if (lastScheduledCompactInstant != null) {
                    while (index < inflightCompactionInstants.size() && compactInstant.equals(lastScheduledCompactInstant)) {
                      index++;
                      LOG.info("Ignoring compaction instant " + compactInstant);
                      compactInstant = inflightCompactionInstants.get(index);
                    }
                    lastScheduledCompactInstant = compactInstant;
                  } else {
                    lastScheduledCompactInstant = compactInstant;
                  }

                  LOG.info(targetPath + " Enqueuing new pending compaction instant (" + lastScheduledCompactInstant + ")");
                  asyncCompactServiceOption.get().enqueuePendingAsyncServiceInstant(new HoodieInstant(HoodieInstant.State.REQUESTED,
                      HoodieTimeline.COMPACTION_ACTION, lastScheduledCompactInstant.getTimestamp()));
                  asyncCompactServiceOption.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingCompactions);
                  if (asyncCompactServiceOption.get().hasError()) {
                    error = true;
                    throw new HoodieException("Async compaction failed.  Shutting down Delta Sync...");
                  }

                  //              if (clusteringConfig.isAsyncClusteringEnabled()) {
                  //                Option<String> clusteringInstant = deltaSync.getClusteringInstantOpt();
                  //                if (clusteringInstant.isPresent()) {
                  //                  LOG.info("Scheduled async clustering for instant: " + clusteringInstant.get());
                  //                  asyncClusteringService.get().enqueuePendingAsyncServiceInstant(
                  //                  new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringInstant.get()));
                  //                  asyncClusteringService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingClustering);
                  //                  if (asyncClusteringService.get().hasError()) {
                  //                    error = true;
                  //                    throw new HoodieException("Async clustering failed.  Shutting down Delta Sync...");
                  //                  }
                  //                }
                  //              }
                  // check if deltastreamer need to be shutdown
                  //                if (postWriteTerminationStrategy.isPresent()) {
                  //                  if (postWriteTerminationStrategy.get().shouldShutdown(lastScheduledCompactInstant.isPresent()
                  //                  ? Option.of(scheduledCompactionInstantAndRDD.get().getRight()) :
                  //                      Option.empty())) {
                  //                    error = true;
                  //                    shutdown(false);
                  //                  }
                  //                }
                }
              }
              long toSleepMs = cfg.minSyncIntervalSeconds * 1000 - (System.currentTimeMillis() - start);
              if (toSleepMs > 0) {
                LOG.info("Last sync ran less than min sync interval: " + cfg.minSyncIntervalSeconds + " s, sleep: "
                    + toSleepMs + " ms.");
                Thread.sleep(toSleepMs);
              }
            } catch (Exception e) {
              LOG.error("Shutting down delta-sync due to exception", e);
              error = true;
              throw new HoodieException(e.getMessage(), e);
            }
          }
        } finally {
          shutdownAsyncServices(error);
          executor.shutdownNow();
        }
        return true;
      }, executor), executor);
    }

    /**
     * Shutdown async services like compaction/clustering as DeltaSync is shutdown.
     */
    private void shutdownAsyncServices(boolean error) {
      LOG.info("Delta Sync shutdown. Error ?" + error);
      asyncCompactServices.values().forEach(asyncCompactService -> {
        if (asyncCompactService.isPresent()) {
          LOG.warn("Gracefully shutting down compactor");
          asyncCompactService.get().shutdown(false);
        }
      });
      //      if (asyncClusteringService.isPresent()) {
      //        LOG.warn("Gracefully shutting down clustering service");
      //        asyncClusteringService.get().shutdown(false);
      //      }
    }

    /**
     * Callback to initialize write client and start compaction service if required.
     *
     * @param writeClient HoodieWriteClient
     * @return
     */
    protected Boolean initializeAsyncTableServices(SparkRDDWriteClient writeClient, String targetPath) {
      if (cfg.isAsyncCompactionEnabled()) {
        if (asyncCompactServices.containsKey(targetPath)) {
          // Update the write client used by Async Compactor.
          LOG.error("Updating write client for async compact service. should not be called");
          asyncCompactServices.get(targetPath).get().updateWriteClient(writeClient);
        } else {
          LOG.info("Initializing async compact service for " + targetPath);
          Option<AsyncCompactService> asyncCompactService = Option.ofNullable(new SparkAsyncCompactService(new HoodieSparkEngineContext(jssc), writeClient));
          asyncCompactServices.put(targetPath, asyncCompactService);
          // Enqueue existing pending compactions first
          HoodieTableMetaClient meta =
              HoodieTableMetaClient.builder().setConf(new Configuration(jssc.hadoopConfiguration())).setBasePath(targetPath).setLoadActiveTimelineOnLoad(true).build();
          List<HoodieInstant> pending = CompactionUtils.getPendingCompactionInstantTimes(meta);
          pending.forEach(hoodieInstant -> asyncCompactService.get().enqueuePendingAsyncServiceInstant(hoodieInstant));
          asyncCompactService.get().start(error -> true);
          try {
            asyncCompactService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingCompactions);
            if (asyncCompactService.get().hasError()) {
              throw new HoodieException("Async compaction failed during write client initialization.");
            }
          } catch (InterruptedException ie) {
            throw new HoodieException(ie);
          }
        }
      }
      // start async clustering if required
      //      if (HoodieClusteringConfig.from(props).isAsyncClusteringEnabled()) {
      //        if (asyncClusteringService.isPresent()) {
      //          asyncClusteringService.get().updateWriteClient(writeClient);
      //        } else {
      //          asyncClusteringService = Option.ofNullable(new SparkAsyncClusteringService(new HoodieSparkEngineContext(jssc), writeClient));
      //          HoodieTableMetaClient meta = HoodieTableMetaClient.builder()
      //              .setConf(new Configuration(jssc.hadoopConfiguration()))
      //              .setBasePath(cfg.targetBasePath)
      //              .setLoadActiveTimelineOnLoad(true).build();
      //          List<HoodieInstant> pending = ClusteringUtils.getPendingClusteringInstantTimes(meta);
      //          LOG.info(String.format("Found %d pending clustering instants ", pending.size()));
      //          pending.forEach(hoodieInstant -> asyncClusteringService.get().enqueuePendingAsyncServiceInstant(hoodieInstant));
      //          asyncClusteringService.get().start(error -> true);
      //          try {
      //            asyncClusteringService.get().waitTillPendingAsyncServiceInstantsReducesTo(cfg.maxPendingClustering);
      //            if (asyncClusteringService.get().hasError()) {
      //              throw new HoodieException("Async clustering failed during write client initialization.");
      //            }
      //          } catch (InterruptedException e) {
      //            throw new HoodieException(e);
      //          }
      //        }
      //      }
      return true;
    }

    /**
     * Close all resources.
     */
    public void close() {
      writeClients.values().forEach(writeClient -> {
        if (writeClient != null) {
          writeClient.close();
        }
      });
    }

    public SparkSession getSparkSession() {
      return sparkSession;
    }

    public TypedProperties getProps() {
      return props;
    }
  }
}
