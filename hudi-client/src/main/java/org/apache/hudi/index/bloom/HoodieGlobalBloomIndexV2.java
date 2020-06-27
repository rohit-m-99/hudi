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

package org.apache.hudi.index.bloom;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieBloomRangeInfoHandle;
import org.apache.hudi.io.HoodieKeyLookupHandle;
import org.apache.hudi.io.HoodieRangeInfoHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Simplified re-implementation of {@link HoodieGlobalBloomIndex} that does not rely on caching, or
 * incurs the overhead of auto-tuning parallelism.
 */
public class HoodieGlobalBloomIndexV2<T extends HoodieRecordPayload> extends HoodieBloomIndexV2<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieGlobalBloomIndexV2.class);

  public HoodieGlobalBloomIndexV2(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
      JavaSparkContext jsc,
      HoodieTable<T> hoodieTable) {

    // collect PairRDD of <partition -> List of BloomIndexFileInfo>
    JavaRDD<PartitionPathBloomInfoList> partitionPathToBloomFileInfo = loadInvolvedFiles(
        jsc, hoodieTable);

    if (partitionPathToBloomFileInfo.count() == 0) {
      // if hudi dataset hasn't been initialized, no files will be found.
      // So, return all incoming records as is
      return recordRDD;
    }

    // cartesian product the partitionBloomInfoList with incoming records, since all records have
    // to be checked against all partitions
    JavaPairRDD<PartitionPathBloomInfoList, HoodieRecord<T>> partitionBloomInfoRecordPair = partitionPathToBloomFileInfo
        .cartesian(recordRDD);

    // sort based on Partition
    JavaRDD<Pair<PartitionPathBloomInfoList, HoodieRecord<T>>> sortedPairs = partitionBloomInfoRecordPair
        .map(
            (Function<Tuple2<PartitionPathBloomInfoList, HoodieRecord<T>>, Pair<PartitionPathBloomInfoList, HoodieRecord<T>>>) v1 -> Pair
                .of(v1._1, v1._2))
        .sortBy(
            (Function<Pair<PartitionPathBloomInfoList, HoodieRecord<T>>, String>) v1 -> v1.getKey()
                .getPartitionPath(), true, config.getBloomIndexV2Parallelism());

    return sortedPairs
        .mapPartitions((itr) -> new LazyRangeAndBloomCheckerGlobal(itr, hoodieTable))
        .flatMap(List::iterator)
        .sortBy(Pair::getRight, true, config.getBloomIndexV2Parallelism())
        .mapPartitions((itr) -> new LazyKeyCheckerGlobal(itr, hoodieTable))
        .flatMap(List::iterator);
  }

  JavaRDD<PartitionPathBloomInfoList> loadInvolvedFiles(final JavaSparkContext jsc,
      final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    try {
      List<String> allPartitionPaths = FSUtils
          .getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
              config.shouldAssumeDatePartitioning());
      return loadInvolvedFilesForPartitons(allPartitionPaths, jsc, hoodieTable);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load all partitions", e);
    }
  }

  JavaRDD<PartitionPathBloomInfoList> loadInvolvedFilesForPartitons(
      List<String> partitions,
      final JavaSparkContext jsc,
      final HoodieTable hoodieTable) {

    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(
        partitions, jsc, hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());

    // if (config.getBloomIndexPruneByRanges()) {
    // also obtain file ranges, if range pruning is enabled
    JavaPairRDD<String, List<BloomIndexFileInfo>> toReturn = jsc
        .parallelize(partitionPathFileIDList, Math.max(partitionPathFileIDList.size(), 1))
        .mapToPair(pf -> {
          try {
            HoodieRangeInfoHandle<T> rangeInfoHandle = new HoodieRangeInfoHandle<T>(config,
                hoodieTable, pf);
            String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
            return new Tuple2<>(pf.getKey(),
                Collections.singletonList(
                    new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1])));
          } catch (MetadataNotFoundException me) {
            LOG.warn("Unable to find range metadata in file :" + pf);
            return new Tuple2<>(pf.getKey(),
                Collections.singletonList(new BloomIndexFileInfo(pf.getValue())));
          }
        });

    return toReturn.reduceByKey(
        (Function2<List<BloomIndexFileInfo>, List<BloomIndexFileInfo>, List<BloomIndexFileInfo>>) (v1, v2) -> {
          List<BloomIndexFileInfo> listToReturn = new ArrayList<>();
          listToReturn.addAll(v1);
          listToReturn.addAll(v2);
          return listToReturn;
        }).map(v1 -> new PartitionPathBloomInfoList(v1._1, v1._2));
    /*} else {
      return partitionPathFileIDList.stream()
          .map(pf -> new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }*/
  }

  /**
   * This is not global, since we depend on the partitionPath to do the lookup.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }


  /**
   * Given an iterator of Pair<PartitionPathBloomInfoList, HoodieRecord<T>, returns a pair of
   * candidate <HoodieRecord, <PartitionPath,FileID>> pairs, by filtering for ranges and bloom for
   * all records with all fileIds.
   */
  class LazyRangeAndBloomCheckerGlobal extends
      LazyIterableIterator<Pair<PartitionPathBloomInfoList, HoodieRecord<T>>, List<Pair<HoodieRecord<T>, Pair<String, String>>>> {

    private HoodieTable<T> table;
    private String currentPartitionPath;
    private ExternalSpillableMap<String, BloomFilter> fileIDToBloomFilter;
    private IndexFileFilter indexFileFilter;
    private HoodieTimer hoodieTimer;
    private long totalTimeMs;
    private long totalCount;
    private long totalMetadataReadTimeMs;
    private long totalRangeCheckTimeMs;
    private long totalBloomCheckTimeMs;
    private long totalMatches;

    public LazyRangeAndBloomCheckerGlobal(
        Iterator<Pair<PartitionPathBloomInfoList, HoodieRecord<T>>> in,
        final HoodieTable<T> table) {
      super(in);
      this.table = table;
    }

    @Override
    protected List<Pair<HoodieRecord<T>, Pair<String, String>>> computeNext() {

      List<Pair<HoodieRecord<T>, Pair<String, String>>> candidates = new ArrayList<>();
      if (!inputItr.hasNext()) {
        return candidates;
      }

      Pair<PartitionPathBloomInfoList, HoodieRecord<T>> partitionBloomInfosRecordTuple = inputItr
          .next();
      HoodieRecord<T> record = partitionBloomInfosRecordTuple.getValue();
      try {
        hoodieTimer.startTimer();
        initIfNeeded(partitionBloomInfosRecordTuple.getKey());
        totalMetadataReadTimeMs += hoodieTimer.endTimer();
        hoodieTimer.startTimer();
      } catch (IOException e) {
        throw new HoodieIOException("Error reading index metadata for " + record.getPartitionPath(),
            e);
      }
      // <Partition path, file name>
      Set<Pair<String, String>> matchingFiles = indexFileFilter
          .getMatchingFilesAndPartition(currentPartitionPath, record.getRecordKey());

      totalRangeCheckTimeMs += hoodieTimer.endTimer();
      hoodieTimer.startTimer();

      matchingFiles.forEach(partitionFileIdPair -> {
        BloomFilter filter = fileIDToBloomFilter.get(partitionFileIdPair.getRight());
        if (filter.mightContain(record.getRecordKey())) {
          totalMatches++;
          candidates.add(Pair.of(record, partitionFileIdPair));
        }
      });
      totalBloomCheckTimeMs += hoodieTimer.endTimer();

      if (candidates.isEmpty()) {
        if (record.getPartitionPath().equalsIgnoreCase(currentPartitionPath)) {
          candidates.add(Pair.of(record, Pair.of("", "")));
        } else {
          // do not add empty Pair if partition path mismatches and no matching files found from
          // range and bloom look up. If not, every record to be tagged for first time, will result
          // in candidates from all partitions since this is global search
        }
      }

      totalCount++;
      return candidates;
    }

    @Override
    protected void start() {
      totalTimeMs = 0;
      totalMatches = 0;
      totalCount = 0;
      hoodieTimer = new HoodieTimer().startTimer();
      totalMetadataReadTimeMs += hoodieTimer.endTimer();
      hoodieTimer.startTimer();
    }

    @Override
    protected void end() {
      totalTimeMs = hoodieTimer.endTimer();
      String rangeCheckInfo = "LazyRangeAndBloomChecker: "
          + "totalCount: " + totalCount + ", "
          + "totalMatches: " + totalMatches + ", "
          + "totalTimeMs: " + totalTimeMs + "ms, "
          + "totalMetadataReadTimeMs: " + totalMetadataReadTimeMs + "ms, "
          + "totalRangeCheckTimeMs: " + totalRangeCheckTimeMs + "ms, "
          + "totalBloomCheckTimeMs: " + totalBloomCheckTimeMs + "ms";
      LOG.info(rangeCheckInfo);
    }

    /**
     * Initialize for the partition path for the new record if need be. If this is a new partition
     * path, fileIdsToBloom need to be populated and instantiate IndexFileFilter
     *
     * @param partitionFileInfos partition path to list of bloomInfo of interest
     */
    private void initIfNeeded(PartitionPathBloomInfoList partitionFileInfos)
        throws IOException {
      if (!Objects.equals(currentPartitionPath, partitionFileInfos.getPartitionPath())) {
        this.currentPartitionPath = partitionFileInfos.getPartitionPath();
        if (fileIDToBloomFilter != null) {
          fileIDToBloomFilter.clear();
        } else {
          fileIDToBloomFilter = new ExternalSpillableMap<>(config.getBloomIndexV2BufferMaxSize(),
              config.getSpillableMapBasePath(), new DefaultSizeEstimator<>(),
              new DefaultSizeEstimator<>());
        }

        List<BloomIndexFileInfo> bloomIndexFileInfoList = partitionFileInfos
            .getBloomIndexFileInfoList();

        // populate fileIDToBloomFilter
        bloomIndexFileInfoList.forEach(fileInfo -> {
          HoodieBloomRangeInfoHandle<T> indexMetadataHandle = new HoodieBloomRangeInfoHandle<T>(
              config, table, Pair.of(currentPartitionPath, fileInfo.getFileId()));
          this.fileIDToBloomFilter.put(fileInfo.getFileId(), indexMetadataHandle.getBloomFilter());
        });
        // instantiate indexFileFilter
        this.indexFileFilter = new IntervalTreeBasedGlobalIndexFileFilter(
            Collections.singletonMap(currentPartitionPath, bloomIndexFileInfoList));
      }
    }
  }

  /**
   * Double check each HoodieRecord by key. 1. return empty if the record doesn't exist in target
   * file slice. 2. tag the matched record with location.
   */
  class LazyKeyCheckerGlobal extends
      LazyIterableIterator<Pair<HoodieRecord<T>, Pair<String, String>>, List<HoodieRecord<T>>> {

    private HoodieKeyLookupHandle<T> currHandle = null;
    private HoodieTable<T> table;
    private HoodieTimer hoodieTimer;
    private long totalTimeMs;
    private long totalCount;
    private long totalReadTimeMs;

    public LazyKeyCheckerGlobal(Iterator<Pair<HoodieRecord<T>, Pair<String, String>>> in,
        HoodieTable<T> table) {
      super(in);
      this.table = table;
    }

    @Override
    protected List<HoodieRecord<T>> computeNext() {
      if (!inputItr.hasNext()) {
        return Collections.emptyList();
      }

      final List<HoodieRecord<T>> toReturn = new ArrayList<>();
      final Pair<HoodieRecord<T>, Pair<String, String>> recordAndFileId = inputItr.next();
      final Pair<String, String> partitionPathFileIdPair = recordAndFileId.getRight();
      final HoodieRecord<T> record = recordAndFileId.getLeft();

      if (partitionPathFileIdPair == null || partitionPathFileIdPair.equals(Pair.of("", ""))) {
        toReturn.add(record);
      } else {
        hoodieTimer.startTimer();
        totalCount++;
        if (currHandle == null || !currHandle.getFileId()
            .equals(partitionPathFileIdPair.getValue())) {
          currHandle = new HoodieKeyLookupHandle<>(config, table, partitionPathFileIdPair);
        }
        totalReadTimeMs += hoodieTimer.endTimer();
        hoodieTimer.startTimer();
        if (currHandle.containsKey(record.getRecordKey())) {
          // if partition path matches
          if (record.getPartitionPath()
              .equalsIgnoreCase(currHandle.getPartitionPathFilePair().getLeft())) {
            HoodieRecordLocation recordLocation = new HoodieRecordLocation(
                currHandle.getBaseInstantTime(), currHandle.getFileId());
            toReturn.add(HoodieIndexUtils.getTaggedRecord(record, Option.of(recordLocation)));
          } else {
            // if partition path mismatches and update partition path set to true
            if (config.getBloomIndexUpdatePartitionPath()) {
              // Create an empty record to delete the record in the old partition
              HoodieRecord<T> emptyRecord = new HoodieRecord(new HoodieKey(record.getRecordKey(),
                  currHandle.getPartitionPathFilePair().getKey()),
                  new EmptyHoodieRecordPayload());
              toReturn.add(emptyRecord);
              // Tag the incoming record for inserting to the new partition
              HoodieRecord<T> taggedRecord = HoodieIndexUtils
                  .getTaggedRecord(record, Option.empty());
              toReturn.add(taggedRecord);
            } else {
              // if partition path mismatches and update partition path set to false,
              // tag incoming record w/ existing location in storage
              HoodieRecordLocation recordLocation = new HoodieRecordLocation(
                  currHandle.getBaseInstantTime(), currHandle.getFileId());
              toReturn.add(HoodieIndexUtils.getTaggedRecord(new HoodieRecord(
                      new HoodieKey(record.getRecordKey(),
                          currHandle.getPartitionPathFilePair().getKey()), record.getData()),
                  Option.of(recordLocation)));
            }
          }
        }
      }
      return toReturn;
    }

    @Override
    protected void start() {
      totalCount = 0;
      totalTimeMs = 0;
      hoodieTimer = new HoodieTimer().startTimer();
    }

    @Override
    protected void end() {
      this.totalTimeMs = hoodieTimer.endTimer();
      LOG.info("LazyKeyChecker: totalCount: " + totalCount + ", totalTimeMs: " + totalTimeMs
          + "ms, totalReadTimeMs:" + totalReadTimeMs + "ms");
    }
  }
}