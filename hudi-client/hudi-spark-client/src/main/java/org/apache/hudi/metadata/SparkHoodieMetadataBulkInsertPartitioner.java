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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * A {@code BulkInsertPartitioner} implementation for Metadata Table to improve performance of initialization of metadata
 * table partition when a very large number of records are inserted.
 * <p>
 * This partitioner requires the records to tbe already tagged with the appropriate file slice.
 */
public class SparkHoodieMetadataBulkInsertPartitioner implements BulkInsertPartitioner<JavaRDD<HoodieRecord>>, Serializable {

  private class FileGroupPartitioner extends Partitioner {
    private int numFileGroups;

    public FileGroupPartitioner(int numFileGroups) {
      this.numFileGroups = numFileGroups;
    }

    @Override
    public int getPartition(Object key) {
      return ((Tuple2<Integer, String>) key)._1;
    }

    @Override
    public int numPartitions() {
      return numFileGroups;
    }
  }

  // The file group count in the partition
  //private int fileGroupCount;
  //private MetadataPartitionType metadataPartitionType;
  // FileIDs for the various partitions
  private List<String> fileIDPfxs = new ArrayList<>();
  private Map<Tuple2<MetadataPartitionType, String>, Integer> fileGroupToPartitionerIndex;
  private Map<MetadataPartitionType, Integer> partitionFileGroupCount;

  public SparkHoodieMetadataBulkInsertPartitioner() {
    //this.metadataPartitionType = metadataPartitionType;
    //this.fileGroupCount = fileGroupCount;
    this.fileGroupToPartitionerIndex = new LinkedHashMap<>();
    this.partitionFileGroupCount = new HashMap<>();
  }

  @Override
  public JavaRDD<HoodieRecord> repartitionRecords(JavaRDD<HoodieRecord> records, int outputSparkPartitions) {
//    Comparator<Tuple2<Integer, String>> keyComparator = (Comparator<Tuple2<Integer, String>> & Serializable) (t1, t2) -> {
//      return t1._2.compareTo(t2._2);
//    };

    List<HoodieRecord> recordsList = records.collect();

    // Partition the records by their location
    /*JavaRDD<HoodieRecord> partitionedRDD = records
         // as of now, keyed by file group index. since we know all records belong to just 1 partition. and values are record keys.
        // we need to fix this. key has to be file group id. Tuple<String, String>
        .keyBy(r -> new Tuple2<Integer, String>(HoodieMetadataFileSliceUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(), fileGroupCount), r.getRecordKey()))
        .repartitionAndSortWithinPartitions(new FileGroupPartitioner(fileGroupCount), keyComparator)
        .map(t -> t._2);
    ValidationUtils.checkArgument(partitionedRDD.getNumPartitions() <= fileGroupCount,
        String.format("Partitioned RDD has more partitions %d than the fileGroupCount %d", partitionedRDD.getNumPartitions(), fileGroupCount));*/

    List<Tuple2<MetadataPartitionType, String>> partitionFileIdList = records
        .mapToPair(rec -> new Tuple2(MetadataPartitionType.valueOf(rec.getPartitionPath()), rec.getCurrentLocation().getFileId()))
        .distinct().collect();

    // assign spark partition index
    int counter = 0;
    for (Tuple2<MetadataPartitionType, String> entry : partitionFileIdList) {
      fileGroupToPartitionerIndex.put(entry, counter++);
      if (partitionFileGroupCount.containsKey(entry._1)) {
        partitionFileGroupCount.put(entry._1, partitionFileGroupCount.get(entry._1) + 1);
      } else {
        partitionFileGroupCount.put(entry._1, 1);
      }
    }

    JavaRDD<HoodieRecord> partitionedRDD = records.mapToPair(rec -> new Tuple2<>(rec, new Tuple2(MetadataPartitionType.valueOf(rec.getPartitionPath()), rec.getCurrentLocation().getFileId())))
        .repartitionAndSortWithinPartitions(new Partitioner() {
          @Override
          public int numPartitions() {
            return fileGroupToPartitionerIndex.size();
          }

          @Override
          public int getPartition(Object key) {
            return fileGroupToPartitionerIndex.get(((Tuple2<HoodieRecord, Tuple2<MetadataPartitionType, String>>) key)._2);
          }
        }, Comparator.comparing(HoodieRecord::getRecordKey)).map(entry -> entry._1);

//
//            JavaRDD < HoodieRecord > partitionedRDD1 = records
//                .map(rec -> new Tuple2(rec, rec.getCurrentLocation().getFileId()))
//                .keyBy(recFileIDPair -> new Tuple2<String, String>((String) recFileIDPair._2, (String) recFileIDPair._1))
//                .repartitionAndSortWithinPartitions(new Partitioner() {
//                  @Override
//                  public int numPartitions() {
//                    return fileGroupToPartitionerIndex.size();
//                  }
//
//                  @Override
//                  public int getPartition(Object key) {
//                    return fileGroupToPartitionerIndex.get(((Tuple2<String, String>) key)._1);
//                  }
//                }, new Comparator<Tuple2<String, String>>() {
//                  @Override
//                  public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
//                    return o1._2.compareTo(o2._2);
//                  }
//                }).map(t -> {
//                  Tuple2<HoodieRecord, String> recFileIdPair = t._2;
//                  return recFileIdPair._1;
//                });

    List<HoodieRecord> recs = partitionedRDD.collect();

    fileIDPfxs = partitionedRDD.mapPartitions(recordItr -> {
      // Due to partitioning, all records in a given spark partition should have same fileID
      List<String> fileIds = new ArrayList<>(1);
      if (recordItr.hasNext()) {
        HoodieRecord record = recordItr.next();
        final String fileID = record.getCurrentLocation().getFileId();
        // Remove the write-token from the fileID as we need to return only the prefix
        int index = fileID.lastIndexOf("-");
        fileIds.add(fileID.substring(0, index));
      }
      // Remove the file-index since we want to
      return fileIds.iterator();
    }, true).collect();
    if (!partitionedRDD.isEmpty()) {
      ValidationUtils.checkArgument(partitionedRDD.getNumPartitions() == fileIDPfxs.size(),
          String.format("Generated fileIDPfxs (%d) are lesser in size than the partitions %d", fileIDPfxs.size(), partitionedRDD.getNumPartitions()));
    } else {
      // TODO: fix here to ensure we can accomodate N partitions.
      fileIDPfxs = new ArrayList<>();
      for (Map.Entry<MetadataPartitionType, Integer> partitionFileGroupCount : partitionFileGroupCount.entrySet()) {
        for (int j = 0; j < partitionFileGroupCount.getValue(); j++) {
          fileIDPfxs.add(String.format("%s%04d-0", partitionFileGroupCount.getKey(), j));
        }
      }
//
//      for (Map.Entry<Tuple2<MetadataPartitionType, String>, Integer> entry : fileGroupToPartitionerIndex.entrySet()) {
//        fileIDPfxs.add(String.format("%s%04d-0", entry.getKey()._1.getFileIdPrefix(), entry.);
//      }
//
//      for (int i = 0; i < fileGroupToPartitionerIndex.size(); ++i) {
//        // Suffix of -0 is used as the file-index (see HoodieCreateHandle) as we want a single file per fileGroup
//        Map.Entry<Tuple2<MetadataPartitionType, String>, Integer> entry = fileGroupToPartitionerIndex.get(0);
//        fileIDPfxs.add(String.format("%s%04d-0", metadataPartitionType.getFileIdPrefix(), i));
//      }
    }
    return partitionedRDD;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return true;
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    return fileIDPfxs.get(partitionId % fileIDPfxs.size());
  }
}