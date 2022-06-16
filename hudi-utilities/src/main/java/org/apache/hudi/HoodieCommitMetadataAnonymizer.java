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

package org.apache.hudi;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.UtilHelpers;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class HoodieCommitMetadataAnonymizer implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieCommitMetadataAnonymizer.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;

  private HoodieTableMetaClient metaClient;

  public HoodieCommitMetadataAnonymizer(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
  }

  public HoodieCommitMetadataAnonymizer(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.metaClient = HoodieTableMetaClient.builder()
        .setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--assume-date-partitioning"}, description = "Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path."
        + "This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually", required = false)
    public Boolean assumeDatePartitioning = false;

    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "MetadataTableValidatorConfig {\n"
          + "   --base-path " + basePath + ", \n"
          + "   --spark-master " + sparkMaster + ", \n"
          + "   --spark-memory " + sparkMemory + ", \n"
          + "   --assumeDatePartitioning-memory " + assumeDatePartitioning + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
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
      return basePath.equals(config.basePath)
          && Objects.equals(sparkMaster, config.sparkMaster)
          && Objects.equals(sparkMemory, config.sparkMemory)
          && Objects.equals(assumeDatePartitioning, config.assumeDatePartitioning)
          && Objects.equals(configs, config.configs);
    }

    @Override
    public int hashCode() {
      return Objects.hash(basePath, sparkMaster, sparkMemory, assumeDatePartitioning, configs, help);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("Hoodie-Metadata-Table-Validator", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    HoodieCommitMetadataAnonymizer validator = new HoodieCommitMetadataAnonymizer(jsc, cfg);
    try {
      validator.run();
    } catch (Throwable throwable) {
      LOG.error("Fail to do hoodie metadata table validation for " + validator.cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() {
    try {
      LOG.info(cfg);
      LOG.info(" ****** do hoodie metadata table validation once ******");
      doHoodieMetadataTableValidationOnce();
    } catch (Exception e) {
      throw new HoodieException("Unable to do hoodie metadata table validation in " + cfg.basePath, e);
    }
  }

  private void doHoodieMetadataTableValidationOnce() {
    try {
      doMetadataTableValidation();
    } catch (HoodieValidationException e) {
      LOG.error("Metadata table validation failed to HoodieValidationException", e);
      throw e;
    }
  }

  public void doMetadataTableValidation() {
    metaClient.reloadActiveTimeline();
    String basePath = metaClient.getBasePath();
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    List<String> allPartitionPathsFromFS = FSUtils.getAllPartitionPaths(engineContext, basePath, false, cfg.assumeDatePartitioning);

    List<HoodieInstant> hoodieInstants = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().collect(Collectors.toList());
    Map<HoodieInstant, CommitMetaInfo> commitMetaInfoMap = new HashMap<>();
    hoodieInstants.forEach(entry -> {
      try {
        switch (entry.getAction()) {
          case HoodieTimeline.DELTA_COMMIT_ACTION:
          case HoodieTimeline.COMMIT_ACTION:
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
                .fromBytes(metaClient.getActiveTimeline().getInstantDetails(entry).get(), HoodieCommitMetadata.class);
            commitMetaInfoMap.put(entry, new CommitMetaInfo(commitMetadata, allPartitionPathsFromFS));
            break;
          default:
            LOG.warn("Ignoring hoodie instant " + entry.toString());
        }
      } catch (IOException io) {
        throw new HoodieIOException("Unable to read metadata for instant " + entry, io);
      }
    });
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

    commitMetaInfoMap.entrySet().forEach((kv) -> {
      try {
        LOG.warn("Commit " + kv.getKey() + "\n " + ow.writeValueAsString(kv.getValue()));
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      LOG.warn("\n\n");
    });
    LOG.info(String.format("Metadata table validation succeeded"));
  }

  class CommitMetaInfo {
    public Map<String, Long> insertRecords = new TreeMap<>();
    public Map<String, Double> insertSpread = new TreeMap<>();
    public Map<String, Long> updateRecords = new TreeMap<>();
    public Map<String, Double> updateSpread = new TreeMap<>();
    public Map<String, Long> deleteRecords = new TreeMap<>();
    public Map<String, Double> deleteSpread = new TreeMap<>();
    public Map<String, Long> maxFileSizeMap = new HashMap<>();
    public long maxFileSize = 0;
    public long totalInserts;
    public long totalUpdates;
    public long totalDeletes;
    public List<String> insertedPartitions = new ArrayList<>();
    public List<String> updatedPartitions = new ArrayList<>();
    public List<String> deletedPartitions = new ArrayList<>();
    public double insertOverAllPerc;
    public double updatesOverallPerc;
    public double deletesOverAllPerc;
    public double updatedDeletesOverAllPerc;

    CommitMetaInfo(HoodieCommitMetadata commitMetadata, List<String> allPartitions) {

      commitMetadata.getPartitionToWriteStats().entrySet().forEach((kv) -> {
        String partition = kv.getKey();
        List<HoodieWriteStat> writeStats = kv.getValue();
        AtomicLong totalInsertsThisPartition = new AtomicLong();
        AtomicLong totalUpdatesThisPartition = new AtomicLong();
        AtomicLong totalDeletesThisPartition = new AtomicLong();
        writeStats.forEach(writeStat -> {
          long inserts = writeStat.getNumInserts();
          long updates = writeStat.getNumUpdateWrites();
          long deletes = writeStat.getNumDeletes();
          totalInsertsThisPartition.addAndGet(inserts);
          totalUpdatesThisPartition.addAndGet(updates);
          totalDeletesThisPartition.addAndGet(deletes);
          if (inserts > 0) {
            insertedPartitions.add(partition);
          }
          if (!insertRecords.containsKey(partition)) {
            insertRecords.put(partition, inserts);
          } else {
            insertRecords.put(partition, insertRecords.get(partition) + inserts);
          }
          if (updates > 0) {
            updatedPartitions.add(partition);
          }
          if (!updateRecords.containsKey(partition)) {
            updateRecords.put(partition, updates);
          } else {
            updateRecords.put(partition, updateRecords.get(partition) + updates);
          }

          if (deletes > 0) {
            deletedPartitions.add(partition);
          }
          if (!deleteRecords.containsKey(partition)) {
            deleteRecords.put(partition, deletes);
          } else {
            deleteRecords.put(partition, deleteRecords.get(partition) + deletes);
          }

          if (!maxFileSizeMap.containsKey(partition)) {
            maxFileSizeMap.put(partition, writeStat.getFileSizeInBytes());
          } else {
            if (writeStat.getFileSizeInBytes() > maxFileSizeMap.get(partition)) {
              maxFileSizeMap.put(partition, writeStat.getFileSizeInBytes());
            }
          }
          maxFileSize = Math.max(maxFileSize, writeStat.getFileSizeInBytes());
        });

        totalInserts += totalInsertsThisPartition.get();
        totalUpdates += totalUpdatesThisPartition.get();
        totalDeletes += totalDeletesThisPartition.get();
      });

      allPartitions.forEach(entry -> {
        if (!insertRecords.containsKey(entry)) {
          insertRecords.put(entry, 0L);
        }
        if (!updateRecords.containsKey(entry)) {
          updateRecords.put(entry, 0L);
        }
        if (!deleteRecords.containsKey(entry)) {
          deleteRecords.put(entry, 0L);
        }
      });

      // find spread.
      insertRecords.entrySet().forEach((kv) -> {
        String partition = kv.getKey();
        long totalInsertsThisPartition = kv.getValue();
        if (totalInserts == 0) {
          insertSpread.put(partition, 0.0);
        } else {
          insertSpread.put(partition, totalInsertsThisPartition * 1.0 / totalInserts);
        }
      });

      updateRecords.entrySet().forEach((kv) -> {
        String partition = kv.getKey();
        long totalInsertsThisPartition = kv.getValue();
        if (totalUpdates == 0) {
          updateSpread.put(partition, 0.0);
        } else {
          updateSpread.put(partition, totalInsertsThisPartition * 1.0 / totalUpdates);
        }
      });

      deleteRecords.entrySet().forEach((kv) -> {
        String partition = kv.getKey();
        long totalInsertsThisPartition = kv.getValue();
        if (totalDeletes == 0) {
          deleteSpread.put(partition, 0.0);
        } else {
          deleteSpread.put(partition, totalInsertsThisPartition * 1.0 / totalDeletes);
        }
      });

      long totalRecords = totalInserts + totalUpdates + totalDeletes;
      insertOverAllPerc = totalInserts * 1.0 / (totalRecords);
      updatesOverallPerc = totalUpdates * 1.0 / (totalRecords);
      deletesOverAllPerc = totalDeletes * 1.0 / (totalRecords);
      updatedDeletesOverAllPerc = 1 - insertOverAllPerc;
    }

    public Map<String, Long> getInsertRecords() {
      return insertRecords;
    }

    public Map<String, Double> getInsertSpread() {
      return insertSpread;
    }

    public Map<String, Long> getUpdateRecords() {
      return updateRecords;
    }

    public Map<String, Double> getUpdateSpread() {
      return updateSpread;
    }

    public Map<String, Long> getDeleteRecords() {
      return deleteRecords;
    }

    public Map<String, Double> getDeleteSpread() {
      return deleteSpread;
    }

    public Map<String, Long> getMaxFileSizeMap() {
      return maxFileSizeMap;
    }

    public long getMaxFileSize() {
      return maxFileSize;
    }

    public long getTotalInserts() {
      return totalInserts;
    }

    public long getTotalUpdates() {
      return totalUpdates;
    }

    public long getTotalDeletes() {
      return totalDeletes;
    }

    public List<String> getInsertedPartitions() {
      return insertedPartitions;
    }

    public List<String> getUpdatedPartitions() {
      return updatedPartitions;
    }

    public List<String> getDeletedPartitions() {
      return deletedPartitions;
    }

    public double getInsertOverAllPerc() {
      return insertOverAllPerc;
    }

    public double getUpdatesOverallPerc() {
      return updatesOverallPerc;
    }

    public double getDeletesOverAllPerc() {
      return deletesOverAllPerc;
    }

    public double getUpdatedDeletesOverAllPerc() {
      return updatedDeletesOverAllPerc;
    }

    @Override
    public String toString() {
      return "CommitMetaInfo{" + "\n"
          + ", insertedPartitions=" + Arrays.toString(insertedPartitions.toArray()) + "\n"
          + ", updatedPartitions=" + Arrays.toString(updatedPartitions.toArray()) + "\n"
          + ", deletedPartitions=" + Arrays.toString(deletedPartitions.toArray()) + "\n"
          + " insertRecords=" + insertRecords.entrySet() + "\n"
          + ", insertSpread=" + insertSpread.entrySet() + "\n"
          + ", updateRecords=" + updateRecords.entrySet() + "\n"
          + ", updateSpread=" + updateSpread.entrySet() + "\n"
          + ", deleteRecords=" + deleteRecords.entrySet() + "\n"
          + ", deleteSpread=" + deleteSpread.entrySet() + "\n"
          + ", maxFileSizeMap=" + maxFileSizeMap.entrySet() + "\n"
          + ", maxFileSize=" + maxFileSize + "\n"
          + ", totalInserts=" + totalInserts + "\n"
          + ", totalUpdates=" + totalUpdates + "\n"
          + ", totalDeletes=" + totalDeletes + "\n"
          + ", insertOverAllPerc=" + insertOverAllPerc + "\n"
          + ", updatesOverallPerc=" + updatesOverallPerc + "\n"
          + ", deletesOverAllPerc=" + deletesOverAllPerc + "\n"
          + ", updatedDeletesOverAllPerc=" + updatedDeletesOverAllPerc + "\n"
          + '}';
    }
  }
}