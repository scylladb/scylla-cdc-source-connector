package com.scylladb.cdc.debezium.connector;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.MasterTransport;
import java.util.SortedSet;

public class ScyllaMasterTransport implements MasterTransport {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final SourceConnectorContext context;
  private final ScyllaConnectorConfig connectorConfig;
  // Vnode-based CDC
  private volatile GroupedTasks globalWorkerTasks;
  // Tablets-based CDC
  private volatile Map<TableName, GroupedTasks> tableWorkerTasks = new ConcurrentHashMap<>();

  public ScyllaMasterTransport(
      SourceConnectorContext context, ScyllaConnectorConfig connectorConfig) {
    this.context = context;
    this.connectorConfig = connectorConfig;
  }

  @Override
  public Optional<GenerationId> getCurrentGenerationId() {
    // TODO - persist generation info - do not start from first generation
    return Optional.empty();
  }

  @Override
  public boolean areTasksFullyConsumedUntil(Set<TaskId> tasks, Timestamp until) {
    OffsetStorageReader reader = context.offsetStorageReader();

    List<Map<String, String>> partitions =
        tasks.stream()
            .map(taskId -> new SourceInfo(connectorConfig, taskId).partition())
            .collect(Collectors.toList());

    Collection<Map<String, Object>> offsets = reader.offsets(partitions).values();

    return offsets.stream().allMatch(o -> isOffsetFullyConsumedUntil(o, until));
  }

  private boolean isOffsetFullyConsumedUntil(Map<String, Object> offset, Timestamp until) {
    if (offset == null) {
      return false;
    }
    UUID offsetUUID = UUID.fromString((String) offset.get(SourceInfo.WINDOW_START));
    Date offsetDate = new Date(UUIDs.unixTimestamp(offsetUUID));
    return offsetDate.after(until.toDate());
  }

  @Override
  public void configureWorkers(GroupedTasks workerTasks) throws InterruptedException {
    this.globalWorkerTasks = workerTasks;
    context.requestTaskReconfiguration();
  }

  @Override
  public void configureWorkers(TableName tableName, GroupedTasks workerTasks)
      throws InterruptedException {
    tableWorkerTasks.put(tableName, workerTasks);
    context.requestTaskReconfiguration();
  }

  @Override
  public Optional<GenerationId> getCurrentGenerationId(TableName tableName) {
    GroupedTasks tasks = tableWorkerTasks.get(tableName);
    return Optional.ofNullable(tasks).map(GroupedTasks::getGenerationId);
  }

  @Override
  public void stopWorkers() throws InterruptedException {
    globalWorkerTasks = null;
    tableWorkerTasks.clear();
    context.requestTaskReconfiguration();
  }

  /**
   * Returns a flattened view of current worker configurations for task assignment.
   * Prefers global (vnode-based) grouped tasks; otherwise merges all per-table grouped tasks.
   */
  public Map<TaskId, SortedSet<StreamId>> getWorkerConfigurations() {
    if (globalWorkerTasks != null) {
      return globalWorkerTasks.getTasks();
    }
    if (!tableWorkerTasks.isEmpty()) {
      return tableWorkerTasks.values().stream()
          .map(GroupedTasks::getTasks)
          .flatMap(m -> m.entrySet().stream())
          .collect(Collectors.toMap(
              Map.Entry::getKey,
              Map.Entry::getValue,
              (existing, replacement) -> {
                logger.warn("TaskId conflict detected when merging worker configurations: TaskId {} appears in multiple tables. Keeping the first occurrence.", existing);
                return existing;
              }
          ));
    }
    return Collections.emptyMap();
  }
}
