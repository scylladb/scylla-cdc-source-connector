package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.pipeline.txmetadata.TransactionContext;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads assigned and legacy tablet offsets without registering legacy offsets as active. */
final class ScyllaOffsetContextLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaOffsetContextLoader.class);

  private ScyllaOffsetContextLoader() {}

  static ScyllaOffsetContext load(
      ScyllaConnectorConfig connectorConfig,
      List<Pair<TaskId, SortedSet<StreamId>>> tasks,
      OffsetStorageReader offsetStorageReader,
      long currentTimeMillis) {
    Map<TaskId, SourceInfo> sourceInfos = new HashMap<>();
    tasks.forEach(
        task -> {
          TaskId taskId = task.getLeft();
          sourceInfos.put(taskId, new SourceInfo(connectorConfig, taskId));
        });

    Set<TaskId> legacyTaskIds =
        sourceInfos.keySet().stream()
            .map(TabletTaskOffsetMigration::legacyTaskId)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Map<TaskId, SourceInfo> offsetSourceInfos = new HashMap<>(sourceInfos);
    legacyTaskIds.forEach(
        taskId -> offsetSourceInfos.put(taskId, new SourceInfo(connectorConfig, taskId)));

    List<Map<String, String>> partitions =
        offsetSourceInfos.values().stream()
            .map(SourceInfo::partition)
            .distinct()
            .collect(Collectors.toList());
    Map<Map<String, String>, Map<String, Object>> offsetMap =
        offsetStorageReader.offsets(partitions);

    Map<TaskId, TaskState> durableStates = new HashMap<>();
    offsetSourceInfos.forEach(
        (taskId, sourceInfo) -> {
          Map<String, Object> offset = offsetMap.get(sourceInfo.partition());
          if (offset != null) {
            durableStates.put(taskId, deserializeTaskState(offset));
          }
        });

    Map<TaskId, TaskState> migrationTaskStates =
        durableStates.entrySet().stream()
            .filter(entry -> legacyTaskIds.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    sourceInfos.forEach(
        (taskId, sourceInfo) -> {
          TaskState assignedState = durableStates.get(taskId);
          if (assignedState != null) {
            sourceInfo.setTaskState(assignedState);
            return;
          }

          boolean hasLegacyState =
              TabletTaskOffsetMigration.legacyTaskId(taskId)
                  .map(migrationTaskStates::containsKey)
                  .orElse(false);
          long lookbackMs = connectorConfig.getInitialLookbackMs();
          if (!hasLegacyState && lookbackMs > 0) {
            long queryWindowMs = connectorConfig.getQueryTimeWindowSizeMs();
            long startMs = currentTimeMillis - lookbackMs;
            long endMs = Math.min(startMs + queryWindowMs, currentTimeMillis);
            sourceInfo.setTaskState(
                new TaskState(
                    new Timestamp(new Date(startMs)),
                    new Timestamp(new Date(endMs)),
                    Optional.empty()));
            LOGGER.info(
                "No saved offset for task {}, applying initial lookback of {} ms",
                taskId,
                lookbackMs);
          }
        });
    return new ScyllaOffsetContext(sourceInfos, migrationTaskStates, new TransactionContext());
  }

  private static TaskState deserializeTaskState(Map<String, Object> offset) {
    Timestamp windowStart =
        new Timestamp(
            new Date(
                UUIDs.unixTimestamp(
                    UUID.fromString((String) offset.get(SourceInfo.WINDOW_START)))));
    Timestamp windowEnd =
        new Timestamp(
            new Date(
                UUIDs.unixTimestamp(UUID.fromString((String) offset.get(SourceInfo.WINDOW_END)))));
    Optional<ChangeId> changeId = Optional.empty();
    if (offset.containsKey(SourceInfo.CHANGE_ID_STREAM_ID)
        && offset.containsKey(SourceInfo.CHANGE_ID_TIME)) {
      StreamId streamId =
          new StreamId(Bytes.fromHexString((String) offset.get(SourceInfo.CHANGE_ID_STREAM_ID)));
      UUID time = UUID.fromString((String) offset.get(SourceInfo.CHANGE_ID_TIME));
      changeId = Optional.of(new ChangeId(streamId, new ChangeTime(time)));
    }
    return new TaskState(windowStart, windowEnd, changeId);
  }
}
