package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.CoordinationGroup;
import com.scylladb.cdc.transport.GroupedTasks;
import com.scylladb.cdc.transport.MasterTransport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaMasterTransport implements MasterTransport {

  static final class WorkerConfigurations {
    private final Map<TaskId, SortedSet<StreamId>> tasks;
    private final Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups;

    private WorkerConfigurations(
        Map<TaskId, SortedSet<StreamId>> tasks,
        Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups) {
      this.tasks = Collections.unmodifiableMap(new LinkedHashMap<>(tasks));
      this.coordinationGroups = Collections.unmodifiableSet(new HashSet<>(coordinationGroups));
    }

    Map<TaskId, SortedSet<StreamId>> getTasks() {
      return tasks;
    }

    Set<CoordinationGroup<TaskId, TaskId>> getCoordinationGroups() {
      return coordinationGroups;
    }
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final SourceConnectorContext context;
  private final ScyllaConnectorConfig connectorConfig;
  // Legacy tablet tasks whose migration was already observed to be over, so that a finished
  // migration is neither looked up nor logged again on every task reconfiguration.
  private final Set<TaskId> completedMigrations = ConcurrentHashMap.newKeySet();
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

    Map<TaskId, Map<String, String>> assignedPartitions = new HashMap<>();
    Map<TaskId, Map<String, String>> legacyPartitions = new HashMap<>();
    tasks.forEach(
        taskId -> {
          assignedPartitions.put(taskId, new SourceInfo(connectorConfig, taskId).partition());
          TabletTaskOffsetMigration.legacyTaskId(taskId)
              .ifPresent(
                  legacyTaskId ->
                      legacyPartitions.put(
                          taskId, new SourceInfo(connectorConfig, legacyTaskId).partition()));
        });

    LinkedHashSet<Map<String, String>> partitions =
        new LinkedHashSet<>(assignedPartitions.values());
    partitions.addAll(legacyPartitions.values());
    Map<Map<String, String>, Map<String, Object>> offsets =
        reader.offsets(new ArrayList<>(partitions));

    return tasks.stream()
        .allMatch(
            taskId -> {
              Map<String, Object> offset = offsets.get(assignedPartitions.get(taskId));
              if (offset == null) {
                Map<String, String> legacyPartition = legacyPartitions.get(taskId);
                if (legacyPartition != null) {
                  offset = offsets.get(legacyPartition);
                }
              }
              return isOffsetFullyConsumedUntil(offset, until);
            });
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

  /** Returns one snapshot of tasks and their authoritative coordination manifests. */
  public WorkerConfigurations getWorkerConfigurations() {
    // Read the volatile field once: stopWorkers() clears it from the master thread while this
    // runs on the herder thread, and a second read could then be null.
    GroupedTasks vnodeTasks = globalWorkerTasks;
    if (vnodeTasks != null) {
      return new WorkerConfigurations(
          vnodeTasks.getTasks(), retainPendingMigrations(vnodeTasks.getCoordinationGroups()));
    }
    if (tableWorkerTasks.isEmpty()) {
      return new WorkerConfigurations(Collections.emptyMap(), Collections.emptySet());
    }

    List<GroupedTasks> groupedTasks = new ArrayList<>(tableWorkerTasks.values());
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups = new HashSet<>();
    groupedTasks.forEach(
        group -> {
          group
              .getTasks()
              .forEach(
                  (taskId, streams) -> {
                    if (tasks.putIfAbsent(taskId, streams) != null) {
                      logger.warn(
                          "TaskId conflict detected when merging worker configurations: TaskId {} appears more than once. Keeping the first occurrence.",
                          taskId);
                    }
                  });
          coordinationGroups.addAll(group.getCoordinationGroups());
        });
    return new WorkerConfigurations(tasks, retainPendingMigrations(coordinationGroups));
  }

  /**
   * Keeps only the coordination manifests that still have something to retire.
   *
   * <p>A manifest lists every participant of its group and is copied into each worker configuration
   * holding one of them, so a high-cardinality tablet table makes it the largest part of every
   * configuration. A manifest is only there to retire a legacy tablet checkpoint, so it is left out
   * once that checkpoint is gone, or once every replacement task stores its own offset and no
   * longer needs it. The library keeps the checkpoint either way.
   *
   * <p>This runs on the herder thread inside {@code taskConfigs()} and looks up one offset per
   * participant, so a group is looked up only until it is done: a migration only ever finishes,
   * because offsets are added and never tombstoned, and a connector restart rebuilds this transport
   * from scratch anyway.
   */
  private Set<CoordinationGroup<TaskId, TaskId>> retainPendingMigrations(
      Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups) {
    Set<CoordinationGroup<TaskId, TaskId>> candidates =
        coordinationGroups.stream()
            .filter(group -> !completedMigrations.contains(group.getKey()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (candidates.isEmpty()) {
      return Collections.emptySet();
    }

    Map<CoordinationGroup<TaskId, TaskId>, Map<String, String>> legacyPartitions =
        new LinkedHashMap<>();
    Map<CoordinationGroup<TaskId, TaskId>, List<Map<String, String>>> participantPartitions =
        new LinkedHashMap<>();
    LinkedHashSet<Map<String, String>> partitions = new LinkedHashSet<>();
    candidates.forEach(
        group -> {
          Map<String, String> legacyPartition =
              new SourceInfo(connectorConfig, group.getKey()).partition();
          legacyPartitions.put(group, legacyPartition);
          partitions.add(legacyPartition);
          List<Map<String, String>> groupPartitions =
              group.getParticipants().stream()
                  .map(participant -> new SourceInfo(connectorConfig, participant).partition())
                  .collect(Collectors.toList());
          participantPartitions.put(group, groupPartitions);
          partitions.addAll(groupPartitions);
        });

    Map<Map<String, String>, Map<String, Object>> offsets;
    try {
      offsets = context.offsetStorageReader().offsets(new ArrayList<>(partitions));
    } catch (RuntimeException e) {
      // Task reconfiguration must not fail over an offset lookup that is only used to leave out
      // manifests that have nothing to do. Keep all of them, as if the checkpoints were pending.
      logger.warn(
          "Could not read the legacy tablet checkpoints; keeping every coordination manifest.", e);
      return candidates;
    }

    Set<CoordinationGroup<TaskId, TaskId>> pending = new HashSet<>();
    int retired = 0;
    for (CoordinationGroup<TaskId, TaskId> group : candidates) {
      if (offsets.get(legacyPartitions.get(group)) == null) {
        completedMigrations.add(group.getKey());
        retired++;
        continue;
      }
      if (participantPartitions.get(group).stream()
          .allMatch(partition -> offsets.get(partition) != null)) {
        if (completedMigrations.add(group.getKey())) {
          logger.info(
              "All {} replacement tasks of {} store their own offset, so its tablet coordination "
                  + "manifest is no longer shipped to the workers. The legacy checkpoint is kept "
                  + "but no longer used.",
              participantPartitions.get(group).size(),
              group.getKey());
        }
        continue;
      }
      pending.add(group);
    }
    if (retired > 0) {
      logger.debug(
          "Skipping {} tablet coordination manifest(s) without a legacy checkpoint to retire.",
          retired);
    }
    return pending;
  }
}
