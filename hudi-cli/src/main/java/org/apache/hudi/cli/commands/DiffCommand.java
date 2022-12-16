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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.cli.utils.CommitUtil.getTimeDaysAgo;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Given a file id or partition value, this command line utility tracks the changes to the file group or partition across range of commits.
 * Usage: diff file --fileId <fileId>
 */
@ShellComponent
public class DiffCommand {

  private static final BiFunction<HoodieWriteStat, String, Boolean> FILE_ID_CHECKER_WITH_HOODIE_WRITE_STAT = (writeStat, fileId) -> fileId.equals(writeStat.getFileId());
  private static final BiFunction<HoodieWriteStat, String, Boolean> PARTITION_CHECKER_WITH_HOODIE_WRITE_STAT = (writeStat, partitionPath) -> partitionPath.equals(writeStat.getPartitionPath());
  private static final BiFunction<HoodieCleanPartitionMetadata, String, Boolean> FILE_ID_CHECKER_WITH_HOODIE_CLEAN_PARTITION_METADATA =
      (cleanPartitionMetadata, fileId) -> cleanPartitionMetadata.getDeletePathPatterns().stream().filter(fileName -> fileName.contains(fileId)).findFirst().isPresent();
  private static final BiFunction<HoodieRollbackPartitionMetadata, String, Boolean> FILE_ID_CHECKER_WITH_HOODIE_ROLLBACK_PARTITION_METADATA =
      (rollbackPartitionMetadata, fileId) -> {
        return rollbackPartitionMetadata.getSuccessDeleteFiles().stream().filter(fileName -> fileName.contains(fileId)).findFirst().isPresent()
           || rollbackPartitionMetadata.getFailedDeleteFiles().stream().filter(fileName -> fileName.contains(fileId)).findFirst().isPresent();
      };

  @ShellMethod(key = "diff file", value = "Check how file differs across range of commits")
  public String diffFile(
      @ShellOption(value = {"--fileId"}, help = "File ID to diff across range of commits") String fileId,
      @ShellOption(value = {"--startTs"}, help = "start time for compactions, default: now - 10 days",
              defaultValue = ShellOption.NULL) String startTs,
      @ShellOption(value = {"--endTs"}, help = "end time for compactions, default: now - 1 day",
              defaultValue = ShellOption.NULL) String endTs,
      @ShellOption(value = {"--limit"}, help = "Limit compactions", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only", defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
          defaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForFileId(timeline, limit, sortByField, descending, headerOnly, "", fileId);
  }

  @ShellMethod(key = "diff partition", value = "Check how file differs across range of commits. It is meant to be used only for partitioned tables.")
  public String diffPartition(
      @ShellOption(value = {"--partitionPath"}, help = "Relative partition path to diff across range of commits") String partitionPath,
      @ShellOption(value = {"--startTs"}, help = "start time for compactions, default: now - 10 days",
              defaultValue = ShellOption.NULL) String startTs,
      @ShellOption(value = {"--endTs"}, help = "end time for compactions, default: now - 1 day",
              defaultValue = ShellOption.NULL) String endTs,
      @ShellOption(value = {"--limit"}, help = "Limit compactions", defaultValue = "-1") final Integer limit,
      @ShellOption(value = {"--sortBy"}, help = "Sorting Field", defaultValue = "") final String sortByField,
      @ShellOption(value = {"--desc"}, help = "Ordering", defaultValue = "false") final boolean descending,
      @ShellOption(value = {"--headeronly"}, help = "Print Header Only", defaultValue = "false") final boolean headerOnly,
      @ShellOption(value = {"--includeArchivedTimeline"}, help = "Include archived commits as well",
          defaultValue = "false") final boolean includeArchivedTimeline) throws IOException {
    HoodieDefaultTimeline timeline = getTimelineInRange(startTs, endTs, includeArchivedTimeline);
    return printCommitsWithMetadataForPartition(timeline, limit, sortByField, descending, headerOnly, "", partitionPath);
  }

  private HoodieDefaultTimeline getTimelineInRange(String startTs, String endTs, boolean includeArchivedTimeline) {
    if (isNullOrEmpty(startTs)) {
      startTs = getTimeDaysAgo(10);
    }
    if (isNullOrEmpty(endTs)) {
      endTs = getTimeDaysAgo(1);
    }
    checkArgument(nonEmpty(startTs), "startTs is null or empty");
    checkArgument(nonEmpty(endTs), "endTs is null or empty");
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    if (includeArchivedTimeline) {
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline();
      archivedTimeline.loadInstantDetailsInMemory(startTs, endTs);
      return archivedTimeline.findInstantsInRange(startTs, endTs).mergeTimeline(activeTimeline);
    }
    return activeTimeline;
  }

  private String printCommitsWithMetadataForFileId(HoodieDefaultTimeline timeline,
                                                   final Integer limit,
                                                   final String sortByField,
                                                   final boolean descending,
                                                   final boolean headerOnly,
                                                   final String tempTableName,
                                                   final String fileId) throws IOException {
    return printDiffWithMetadata(timeline, limit, sortByField, descending, headerOnly, tempTableName, fileId, FILE_ID_CHECKER_WITH_HOODIE_WRITE_STAT,
        FILE_ID_CHECKER_WITH_HOODIE_CLEAN_PARTITION_METADATA, FILE_ID_CHECKER_WITH_HOODIE_ROLLBACK_PARTITION_METADATA);
  }

  private String printCommitsWithMetadataForPartition(HoodieDefaultTimeline timeline,
                                                      final Integer limit,
                                                      final String sortByField,
                                                      final boolean descending,
                                                      final boolean headerOnly,
                                                      final String tempTableName,
                                                      final String partition) throws IOException {
    return printDiffWithMetadata(timeline, limit, sortByField, descending, headerOnly, tempTableName, partition, PARTITION_CHECKER_WITH_HOODIE_WRITE_STAT,
        FILE_ID_CHECKER_WITH_HOODIE_CLEAN_PARTITION_METADATA, FILE_ID_CHECKER_WITH_HOODIE_ROLLBACK_PARTITION_METADATA);
  }

  private String printDiffWithMetadata(HoodieDefaultTimeline timeline, Integer limit, String sortByField, boolean descending, boolean headerOnly, String tempTableName, String diffEntity,
                                       BiFunction<HoodieWriteStat, String, Boolean> fileIdCheckerWithWriteStat,
                                       BiFunction<HoodieCleanPartitionMetadata, String, Boolean> fileIdCheckerWithCleanPartitionMetadata,
                                       BiFunction<HoodieRollbackPartitionMetadata, String, Boolean> fileIdCheckerWithRollbackMetadata) throws IOException {
    List<Comparable[]> rows = new ArrayList<>();
    List<HoodieInstant> instants = timeline.filterCompletedInstants().getInstants().sorted(HoodieInstant.COMPARATOR.reversed()).collect(Collectors.toList());

    for (final HoodieInstant commit : instants) {
      Option<byte[]> instantDetails = timeline.getInstantDetails(commit);
      if (instantDetails.isPresent()) {
        switch (commit.getAction()) {
          case HoodieTimeline.COMMIT_ACTION:
          case HoodieTimeline.DELTA_COMMIT_ACTION:
          case HoodieTimeline.COMPACTION_ACTION:
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(instantDetails.get(), HoodieCommitMetadata.class);
            for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
                commitMetadata.getPartitionToWriteStats().entrySet()) {
              for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
                populateRows(rows, commit, hoodieWriteStat, diffEntity, fileIdCheckerWithWriteStat);
              }
            }
            break;
          case HoodieTimeline.REPLACE_COMMIT_ACTION:
            HoodieReplaceCommitMetadata replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(
                instantDetails.get(), HoodieReplaceCommitMetadata.class);
            for (Map.Entry<String, List<HoodieWriteStat>> partitionWriteStat :
                replaceMetadata.getPartitionToWriteStats().entrySet()) {
              for (HoodieWriteStat hoodieWriteStat : partitionWriteStat.getValue()) {
                populateRows(rows, commit, hoodieWriteStat, diffEntity, fileIdCheckerWithWriteStat);
              }
            }
            // to do add replaced file info.
            break;
          case HoodieTimeline.CLEAN_ACTION:
            HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(instantDetails.get());
            for (Map.Entry<String, HoodieCleanPartitionMetadata> entry : cleanMetadata.getPartitionMetadata().entrySet()) {
              populateCleanRows(rows, commit, entry.getKey(), entry.getValue(), diffEntity, fileIdCheckerWithCleanPartitionMetadata);
            }
            break;
          case HoodieTimeline.ROLLBACK_ACTION:
            HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(instantDetails.get());
            for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.getPartitionMetadata().entrySet()) {
              populateRollbackRows(rows, commit, entry.getKey(), entry.getValue(), diffEntity, fileIdCheckerWithRollbackMetadata);
            }
            break;
          //case HoodieTimeline.RESTORE_ACTION:break;
          default:
            throw new HoodieException("Failed to deser " + commit.toString());
        }
      }
    }

    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(
        HoodieTableHeaderFields.HEADER_TOTAL_BYTES_WRITTEN,
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString()))));

    return HoodiePrintHelper.print(HoodieTableHeaderFields.getTableHeaderWithExtraMetadataWithFileName(),
        fieldNameToConverterMap, sortByField, descending, limit, headerOnly, rows, tempTableName);
  }

  private void populateRows(List<Comparable[]> rows, HoodieInstant instant, HoodieWriteStat hoodieWriteStat,
                            String value, BiFunction<HoodieWriteStat, String, Boolean> checker) {
    if (checker.apply(hoodieWriteStat, value)) {
      rows.add(new Comparable[] {
          instant.getAction(),
          instant.getTimestamp(),
          hoodieWriteStat.getPartitionPath(),
          hoodieWriteStat.getFileId(),
          hoodieWriteStat.getPath(),
          hoodieWriteStat.getPrevCommit(),
          hoodieWriteStat.getNumWrites(),
          hoodieWriteStat.getNumInserts(),
          hoodieWriteStat.getNumDeletes(),
          hoodieWriteStat.getNumUpdateWrites(),
          hoodieWriteStat.getTotalWriteErrors(),
          hoodieWriteStat.getTotalLogBlocks(),
          hoodieWriteStat.getTotalCorruptLogBlock(),
          hoodieWriteStat.getTotalRollbackBlocks(),
          hoodieWriteStat.getTotalLogRecords(),
          hoodieWriteStat.getTotalUpdatedRecordsCompacted(),
          hoodieWriteStat.getTotalWriteBytes()
      });
    }
  }

  private void populateCleanRows(List<Comparable[]> rows, HoodieInstant instant, String partitionPath, HoodieCleanPartitionMetadata cleanPartitionMetadata,
                                 String value, BiFunction<HoodieCleanPartitionMetadata, String, Boolean> checker) {
    if (checker.apply(cleanPartitionMetadata, value)) {
      rows.add(new Comparable[] {
          instant.getAction(),
          instant.getTimestamp(),
          partitionPath,
          cleanPartitionMetadata.getDeletePathPatterns().stream().filter(fileName -> fileName.contains(value)).findFirst().get(),
          cleanPartitionMetadata.getDeletePathPatterns().stream().filter(fileName -> fileName.contains(value)).findFirst().get(),
          null,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0
      });
    }
  }

  private void populateRollbackRows(List<Comparable[]> rows, HoodieInstant instant, String partitionPath, HoodieRollbackPartitionMetadata rollbackPartitionMetadata,
                                 String value, BiFunction<HoodieRollbackPartitionMetadata, String, Boolean> checker) {
    if (checker.apply(rollbackPartitionMetadata, value)) {
      String matchedFileName = null;
      boolean isPresent = rollbackPartitionMetadata.getSuccessDeleteFiles().stream().filter(fileName -> fileName.contains(value)).findFirst().isPresent();
      if (isPresent) {
        matchedFileName = rollbackPartitionMetadata.getSuccessDeleteFiles().stream().filter(fileName -> fileName.contains(value)).findFirst().get();
      } else {
        matchedFileName = rollbackPartitionMetadata.getFailedDeleteFiles().stream().filter(fileName -> fileName.contains(value)).findFirst().get();
      }
      rows.add(new Comparable[] {
          instant.getAction(),
          instant.getTimestamp(),
          partitionPath,
          matchedFileName,
          matchedFileName,
          null,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0
      });
    }
  }

}
