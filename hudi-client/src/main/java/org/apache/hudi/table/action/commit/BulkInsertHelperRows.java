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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class BulkInsertHelperRows {

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata bulkInsertRows(
      Dataset<Row> inputRecords, String instantTime,
      HoodieTable<T> table, HoodieWriteConfig config,
      CommitActionExecutor<T> executor, boolean performDedupe,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    // De-dupe/merge if needed
    Dataset<Row> dedupedRecords = inputRecords;

    /*if (performDedupe) {
      dedupedRecords = WriteHelper.combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          config.getInsertShuffleParallelism(), ((HoodieTable<T>)table));
    }*/

    final int parallelism = config.getBulkInsertShuffleParallelism();
    final Dataset<Row> repartitionedRecords = dedupedRecords.sort(config.getRecordKeyFieldProp(), config.getPartitionPathFieldProp());

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
        table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    List<Attribute> attributes = JavaConversions.asJavaCollection(repartitionedRecords.schema().toAttributes()).stream().map(Attribute::toAttribute).collect(Collectors.toList());
    ExpressionEncoder encoder = RowEncoder.apply(repartitionedRecords.schema())
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(), SimpleAnalyzer$.MODULE$);

    Dataset<WriteStatus> writeStatusRDD = repartitionedRecords.mapPartitions(
        new BulkInsertRowsMapFunction<>(instantTime, config, table, fileIDPrefixes, encoder), Encoders.bean(WriteStatus.class))
        .flatMap(new FlatMapFunction<Iterator<WriteStatus>, WriteStatus>() {
          @Override
          public Iterator<WriteStatus> call(Iterator<WriteStatus> writeStatusIterator) throws Exception {
            return writeStatusIterator;
          }
        }, Encoders.bean(WriteStatus.class));
    // .flatMap(List::iterator);

    executor.updateIndexAndCommitIfNeeded(writeStatusRDD.toJavaRDD(), result);
    return result;



    /*if (bulkInsertPartitioner.isPresent()) {
      repartitionedRecords = bulkInsertPartitioner.get().repartitionRecords(dedupedRecords, parallelism);
    } else {
      // Now, sort the records and line them up nicely for loading.
      repartitionedRecords = dedupedRecords.sortBy(record -> {
        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
        // the records split evenly across RDD partitions, such that small partitions fit
        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
        return String.format("%s+%s", record.getPartitionPath(), record.getRecordKey());
      }, true, parallelism);
    } */

    // generate new file ID prefixes for each output partition
    /*final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
        table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(instantTime, config, table, fileIDPrefixes), true)
        .flatMap(List::iterator);

    executor.updateIndexAndCommitIfNeeded(writeStatusRDD, result);
    return result;*/
  }
}
