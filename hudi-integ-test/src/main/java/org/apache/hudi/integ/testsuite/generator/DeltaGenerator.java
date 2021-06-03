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

package org.apache.hudi.integ.testsuite.generator;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config;
import org.apache.hudi.integ.testsuite.converter.Converter;
import org.apache.hudi.integ.testsuite.converter.DeleteConverter;
import org.apache.hudi.integ.testsuite.converter.UpdateConverter;
import org.apache.hudi.integ.testsuite.reader.DFSAvroDeltaInputReader;
import org.apache.hudi.integ.testsuite.reader.DFSHoodieDatasetInputReader;
import org.apache.hudi.integ.testsuite.reader.DeltaInputReader;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterAdapter;
import org.apache.hudi.integ.testsuite.writer.DeltaWriterFactory;
import org.apache.hudi.keygen.BuiltinKeyGenerator;

<<<<<<< Updated upstream
=======
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
>>>>>>> Stashed changes
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< Updated upstream
=======
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
>>>>>>> Stashed changes

import scala.Tuple2;

/**
 * The delta generator generates all types of workloads (insert, update) for the given configs.
 */
public class DeltaGenerator implements Serializable {

  private static Logger log = LoggerFactory.getLogger(DeltaGenerator.class);

  private DFSDeltaConfig deltaOutputConfig;
  private transient JavaSparkContext jsc;
  private transient SparkSession sparkSession;
  private String schemaStr;
  private List<String> recordRowKeyFieldNames;
  private List<String> partitionPathFieldNames;
<<<<<<< Updated upstream
  private int batchId;
=======
  private int batchId = 0;
  private String preCombineField;
>>>>>>> Stashed changes

  public DeltaGenerator(DFSDeltaConfig deltaOutputConfig, JavaSparkContext jsc, SparkSession sparkSession,
                        String schemaStr, BuiltinKeyGenerator keyGenerator) {
    this.deltaOutputConfig = deltaOutputConfig;
    this.jsc = jsc;
    this.sparkSession = sparkSession;
    this.schemaStr = schemaStr;
    this.recordRowKeyFieldNames = keyGenerator.getRecordKeyFields();
    this.partitionPathFieldNames = keyGenerator.getPartitionPathFields();
  }

  public JavaRDD<DeltaWriteStats> writeRecords(JavaRDD<GenericRecord> records) {
    // log.warn("DeltaGenerator :: Starting to write records to storage for batch " + batchId);
    if (deltaOutputConfig.shouldDeleteOldInputData() && batchId > 1) {
      Path oldInputDir = new Path(deltaOutputConfig.getDeltaBasePath(), Integer.toString(batchId - 1));
      try {
        FileSystem fs = FSUtils.getFs(oldInputDir.toString(), deltaOutputConfig.getConfiguration());
        fs.delete(oldInputDir, true);
      } catch (IOException e) {
        log.error("Failed to delete older input data direcory " + oldInputDir, e);
      }
    }

    // The following creates a new anonymous function for iterator and hence results in serialization issues
    JavaRDD<DeltaWriteStats> ws = records.mapPartitions(itr -> {
      try {
        DeltaWriterAdapter<GenericRecord> deltaWriterAdapter = DeltaWriterFactory
            .getDeltaWriterAdapter(deltaOutputConfig, batchId);
        return Collections.singletonList(deltaWriterAdapter.write(itr)).iterator();
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    }).flatMap(List::iterator);
    // log.warn("DeltaGenerator :: Completed writing records to storage for batch " + batchId);
    batchId++;
    return ws;
  }

  public JavaRDD<GenericRecord> generateInserts(Config operation) {
    return generateInserts(operation, true);
  }

  public JavaRDD<GenericRecord> generateInserts(Config operation, boolean toLog) {
    int numPartitions = operation.getNumInsertPartitions();
    long recordsPerPartition = operation.getNumRecordsInsert() / numPartitions;
    int minPayloadSize = operation.getRecordSize();
    int startPartition = operation.getStartPartition();

    // Each spark partition below will generate records for a single partition given by the integer index.
    List<Integer> partitionIndexes = IntStream.rangeClosed(0 + startPartition, numPartitions + startPartition)
        .boxed().collect(Collectors.toList());

    // log.warn("DeltaGenerator:: generateInserts. Initializing for batchID " + batchId);
    JavaRDD<GenericRecord> inputBatch = jsc.parallelize(partitionIndexes, numPartitions)
        .mapPartitionsWithIndex((index, p) -> {
          return new LazyRecordGeneratorIterator(new FlexibleSchemaRecordGenerationIterator(recordsPerPartition,
<<<<<<< Updated upstream
            minPayloadSize, schemaStr, partitionPathFieldNames, (Integer)index));
=======
              minPayloadSize, schemaStr, partitionPathFieldNames, preCombineField, batchId, (Integer) index));
>>>>>>> Stashed changes
        }, true);

    if (deltaOutputConfig.getInputParallelism() < numPartitions) {
      inputBatch = inputBatch.coalesce(deltaOutputConfig.getInputParallelism());
    }
    /*if (toLog) {
      List<GenericRecord> insertList = inputBatch.collect();
      log.warn(batchId + " DeltaGenerator:: generateInserts :: Insert records :: " + insertList.size());
      for (GenericRecord rec : insertList) {
        log.warn(batchId + " DeltaGenerator:: Insert record -> " + rec.toString());
      }
    }*/

    return inputBatch;
  }

  public JavaRDD<GenericRecord> generateUpdates(Config config) throws IOException {

    // log.warn(batchId + " DeltaGenerator:: generateUpdates:: config " + config.toString());

    if (deltaOutputConfig.getDeltaOutputMode() == DeltaOutputMode.DFS) {
      JavaRDD<GenericRecord> inserts = null;
      DeltaInputReader deltaInputReader = null;
      JavaRDD<GenericRecord> adjustedRDD = null;

      if (config.getNumRecordsInsert() > 0) {
        inserts = generateInserts(config, false);
          /*List<GenericRecord> insertList = inserts.collect();
          log.warn(batchId + " DeltaGenerator:: generateUpdates:: Insert records :: " + insertList.size());
          for (GenericRecord rec : insertList) {
            log.warn(batchId + " DeltaGenerator:: Insert record -> " + rec.toString());
          }*/
      }

      if (config.getNumUpsertPartitions() != 0) {
        // log.warn(batchId + " DeltaGenerator:: generateUpdates:: num records to upsert " + config.getNumRecordsUpsert());
        if (config.getNumUpsertPartitions() < 0) {
          //log.warn(batchId + " DeltaGenerator:: generateUpdates:: getNumUpsertPartitions < 0 ");
          // randomly generate updates for a given number of records without regard to partitions and files
          deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
              ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
          adjustedRDD = deltaInputReader.read(config.getNumRecordsUpsert());
          adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsUpsert());
        } else {
          // log.warn(batchId + " DeltaGenerator:: generateUpdates:: getNumUpsertPartitions > 0 " + config.getNumUpsertPartitions());
          deltaInputReader =
              new DFSHoodieDatasetInputReader(jsc, ((DFSDeltaConfig) deltaOutputConfig).getDatasetOutputPath(),
                  schemaStr);
          if (config.getFractionUpsertPerFile() > 0) {
            // log.warn(batchId + " DeltaGenerator:: generateUpdates:: path aaa ");
            adjustedRDD = deltaInputReader.read(config.getNumUpsertPartitions(), config.getNumUpsertFiles(),
                config.getFractionUpsertPerFile());
          } else {
            // log.warn(batchId + " DeltaGenerator:: generateUpdates:: path bbb ");
            adjustedRDD = deltaInputReader.read(config.getNumUpsertPartitions(), config.getNumUpsertFiles(), config
                .getNumRecordsUpsert());
          }
        }

        // persist this since we will make multiple passes over this
        int numPartition = Math.min(deltaOutputConfig.getInputParallelism(),
            Math.max(1, config.getNumUpsertPartitions()));
        log.info("Repartitioning records into " + numPartition + " partitions for updates");
        adjustedRDD = adjustedRDD.repartition(numPartition);
<<<<<<< Updated upstream
        log.info("Repartitioning records done for updates");

=======
        /*List<GenericRecord> updateList = adjustedRDD.collect();
        log.warn(batchId + " DeltaGenerator:: generateUpdates:: Update records bfr convrtn :: " + updateList.size());
        for (GenericRecord rec : updateList) {
          log.warn(batchId + " DeltaGenerator:: Update record bfr cnvrtn-> " + rec.toString());
        }*/

        log.info("Repartitioning records done for updates for batchId " + batchId);
>>>>>>> Stashed changes
        UpdateConverter converter = new UpdateConverter(schemaStr, config.getRecordSize(),
            partitionPathFieldNames, recordRowKeyFieldNames);
        JavaRDD<GenericRecord> updates = converter.convert(adjustedRDD);
        updates.persist(StorageLevel.DISK_ONLY());
        /*updateList = updates.collect();
        log.warn(batchId + " DeltaGenerator:: generateUpdates:: Update records :: " + updateList.size());
        for (GenericRecord rec : updateList) {
          log.warn(batchId + " DeltaGenerator:: Update record -> " + rec.toString());
        }*/

        if (inserts == null) {
          inserts = updates;
        } else {
          inserts = inserts.union(updates);
        }
      }
      return inserts;
      // TODO : Generate updates for only N partitions.
    } else {
      throw new IllegalArgumentException("Other formats are not supported at the moment");
    }
  }

  public JavaRDD<GenericRecord> generateDeletes(Config config) throws IOException {
    if (deltaOutputConfig.getDeltaOutputMode() == DeltaOutputMode.DFS) {
      DeltaInputReader deltaInputReader = null;
      JavaRDD<GenericRecord> adjustedRDD = null;

      if (config.getNumDeletePartitions() < 1) {
        log.warn("DeltaGenerator:: generateDeletes aaa");
        // randomly generate deletes for a given number of records without regard to partitions and files
        deltaInputReader = new DFSAvroDeltaInputReader(sparkSession, schemaStr,
            ((DFSDeltaConfig) deltaOutputConfig).getDeltaBasePath(), Option.empty(), Option.empty());
        adjustedRDD = deltaInputReader.read(config.getNumRecordsDelete());
        adjustedRDD = adjustRDDToGenerateExactNumUpdates(adjustedRDD, jsc, config.getNumRecordsDelete());
      } else {
        log.warn("DeltaGenerator:: generateDeletes bbb. numDeletes " + config.getNumRecordsDelete() + ", partitions to delete " + config.getNumDeletePartitions());
        deltaInputReader =
            new DFSHoodieDatasetInputReader(jsc, ((DFSDeltaConfig) deltaOutputConfig).getDatasetOutputPath(),
                schemaStr);
        if (config.getFractionUpsertPerFile() > 0) {
          log.warn("DeltaGenerator:: generateDeletes bbb 111 ");
          adjustedRDD = deltaInputReader.read(config.getNumDeletePartitions(), config.getNumUpsertFiles(),
              config.getFractionUpsertPerFile());
        } else {
          log.warn("DeltaGenerator:: generateDeletes bbb 222");
          adjustedRDD = deltaInputReader.read(config.getNumDeletePartitions(), config.getNumUpsertFiles(), config
              .getNumRecordsDelete());
        }
      }
      log.info("Repartitioning records for delete");
      // persist this since we will make multiple passes over this
      adjustedRDD = adjustedRDD.repartition(jsc.defaultParallelism());
      /*List<GenericRecord> deleteList = adjustedRDD.collect();
      log.warn("Delete size 111 :: " + deleteList.size());
      for (GenericRecord rec : deleteList) {
        log.warn("Delete rec 111 -> " + rec.toString());
      }*/

      Converter converter = new DeleteConverter(schemaStr, config.getRecordSize());
      JavaRDD<GenericRecord> deletes = converter.convert(adjustedRDD);
      deletes.persist(StorageLevel.DISK_ONLY());
      /*deleteList = deletes.collect();
      log.warn("Delete size 222 :: " + deleteList.size());
      for (GenericRecord rec : deleteList) {
        log.warn("Delete rec 222 -> " + rec.toString());
      }*/
      return deletes;
    } else {
      throw new IllegalArgumentException("Other formats are not supported at the moment");
    }
  }


  public Map<Integer, Long> getPartitionToCountMap(JavaRDD<GenericRecord> records) {
    // Requires us to keep the partitioner the same
    return records.mapPartitionsWithIndex((index, itr) -> {
      Iterable<GenericRecord> newIterable = () -> itr;
      // parallelize counting for speed
      long count = StreamSupport.stream(newIterable.spliterator(), true).count();
      return Arrays.asList(new Tuple2<>(index, count)).iterator();
    }, true).mapToPair(i -> i).collectAsMap();
  }

  public Map<Integer, Long> getAdjustedPartitionsCount(Map<Integer, Long> partitionCountMap, long
      recordsToRemove) {
    long remainingRecordsToRemove = recordsToRemove;
    Iterator<Map.Entry<Integer, Long>> iterator = partitionCountMap.entrySet().iterator();
    Map<Integer, Long> adjustedPartitionCountMap = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<Integer, Long> entry = iterator.next();
      if (entry.getValue() < remainingRecordsToRemove) {
        remainingRecordsToRemove -= entry.getValue();
        adjustedPartitionCountMap.put(entry.getKey(), 0L);
      } else {
        long newValue = entry.getValue() - remainingRecordsToRemove;
        remainingRecordsToRemove = 0;
        adjustedPartitionCountMap.put(entry.getKey(), newValue);
      }
      if (remainingRecordsToRemove == 0) {
        break;
      }
    }
    return adjustedPartitionCountMap;
  }

  public JavaRDD<GenericRecord> adjustRDDToGenerateExactNumUpdates(JavaRDD<GenericRecord> updates, JavaSparkContext
      jsc, long totalRecordsRequired) {
    Map<Integer, Long> actualPartitionCountMap = getPartitionToCountMap(updates);
    long totalRecordsGenerated = actualPartitionCountMap.values().stream().mapToLong(Long::longValue).sum();
    if (isSafeToTake(totalRecordsRequired, totalRecordsGenerated)) {
      // Generate totalRecordsRequired - totalRecordsGenerated new records and union the RDD's
      // NOTE : This performs poorly when totalRecordsRequired >> totalRecordsGenerated. Hence, always
      // ensure that enough inserts are created before hand (this needs to be noted during the WorkflowDag creation)
      long sizeOfUpdateRDD = totalRecordsGenerated;
      while (totalRecordsRequired != sizeOfUpdateRDD) {
        long recordsToTake = (totalRecordsRequired - sizeOfUpdateRDD) > sizeOfUpdateRDD
            ? sizeOfUpdateRDD : (totalRecordsRequired - sizeOfUpdateRDD);
        if ((totalRecordsRequired - sizeOfUpdateRDD) > recordsToTake && recordsToTake <= sizeOfUpdateRDD) {
          updates = updates.union(updates);
          sizeOfUpdateRDD *= 2;
        } else {
          List<GenericRecord> remainingUpdates = updates.take((int) (recordsToTake));
          updates = updates.union(jsc.parallelize(remainingUpdates));
          sizeOfUpdateRDD = sizeOfUpdateRDD + recordsToTake;
        }
      }
      return updates;
    } else if (totalRecordsRequired < totalRecordsGenerated) {
      final Map<Integer, Long> adjustedPartitionCountMap = getAdjustedPartitionsCount(actualPartitionCountMap,
          totalRecordsGenerated - totalRecordsRequired);
      // limit counts across partitions to meet the exact number of updates required
      JavaRDD<GenericRecord> trimmedRecords = updates.mapPartitionsWithIndex((index, itr) -> {
        int counter = 1;
        List<GenericRecord> entriesToKeep = new ArrayList<>();
        if (!adjustedPartitionCountMap.containsKey(index)) {
          return itr;
        } else {
          long recordsToKeepForThisPartition = adjustedPartitionCountMap.get(index);
          while (counter <= recordsToKeepForThisPartition && itr.hasNext()) {
            entriesToKeep.add(itr.next());
            counter++;
          }
          return entriesToKeep.iterator();
        }
      }, true);
      return trimmedRecords;
    }
    return updates;
  }

  private boolean isSafeToTake(long totalRecords, long totalRecordsGenerated) {
    // TODO : Ensure that the difference between totalRecords and totalRecordsGenerated is not too big, if yes,
    // then there are fewer number of records on disk, hence we need to find another way to generate updates when
    // requiredUpdates >> insertedRecords
    return totalRecords > totalRecordsGenerated;
  }

}
