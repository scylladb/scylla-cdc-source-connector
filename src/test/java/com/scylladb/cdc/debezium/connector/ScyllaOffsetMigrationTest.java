package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.config.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

class ScyllaOffsetMigrationTest {

  private static final long GENERATION_START_MS = 1_000;
  private static final TableName TABLE = new TableName("ks", "table");
  private static final GenerationId GENERATION =
      new GenerationId(new Timestamp(new Date(GENERATION_START_MS)));

  @Test
  void legacyOffsetIsLoadedSeparatelyAndSuppressesLookback() {
    ScyllaConnectorConfig config = connectorConfig(5_000, 1_000);
    TaskId task = tabletTask(0);
    TaskId legacyTask = legacyTask(task);
    OffsetReaderFixture offsets =
        new OffsetReaderFixture(
            Map.of(new SourceInfo(config, legacyTask).partition(), offset(2_000, 3_000)));

    ScyllaOffsetContext offsetContext =
        ScyllaOffsetContextLoader.load(config, tasks(task), offsets.reader, 10_000);

    assertNull(offsetContext.taskStateOffsetContext(task).getTaskState());
    Map<TaskId, TaskState> migrationStates =
        new ScyllaWorkerTransport(null, offsetContext, null, 0)
            .getTaskStatesForMigration(Set.of(legacyTask));
    assertTaskState(migrationStates.get(legacyTask), 2_000, 3_000);
    assertThrows(
        UnsupportedOperationException.class,
        () -> migrationStates.put(legacyTask, migrationStates.get(legacyTask)));

    Set<Map<String, String>> publishedPartitions =
        offsetContext.toDebeziumOffsets().getPartitions().stream()
            .map(ScyllaPartition::getSourcePartition)
            .collect(Collectors.toSet());
    assertEquals(Set.of(new SourceInfo(config, task).partition()), publishedPartitions);
    assertEquals(
        Set.of(
            new SourceInfo(config, task).partition(),
            new SourceInfo(config, legacyTask).partition()),
        offsets.onlyRequestedPartitions());
  }

  @Test
  void partialAssignedOffsetsRemainAuthoritativeWhileLegacyCoversMissingTasks() {
    ScyllaConnectorConfig config = connectorConfig(5_000, 1_000);
    TaskId restoredTask = tabletTask(0);
    TaskId missingTask = tabletTask(1);
    TaskId legacyTask = legacyTask(restoredTask);
    Map<Map<String, String>, Map<String, Object>> storedOffsets = new HashMap<>();
    storedOffsets.put(new SourceInfo(config, restoredTask).partition(), offset(7_000, 8_000));
    storedOffsets.put(new SourceInfo(config, legacyTask).partition(), offset(2_000, 3_000));
    OffsetReaderFixture offsets = new OffsetReaderFixture(storedOffsets);

    ScyllaOffsetContext offsetContext =
        ScyllaOffsetContextLoader.load(
            config, tasks(restoredTask, missingTask), offsets.reader, 10_000);
    ScyllaWorkerTransport transport = new ScyllaWorkerTransport(null, offsetContext, null, 0);

    Map<TaskId, TaskState> assignedStates =
        transport.getTaskStates(Set.of(restoredTask, missingTask));
    assertEquals(Set.of(restoredTask), assignedStates.keySet());
    assertTaskState(assignedStates.get(restoredTask), 7_000, 8_000);
    assertNull(offsetContext.taskStateOffsetContext(missingTask).getTaskState());
    assertTaskState(
        transport.getTaskStatesForMigration(Set.of(legacyTask)).get(legacyTask), 2_000, 3_000);
  }

  @Test
  void lookbackIsUsedWhenNoDurableOffsetExists() {
    ScyllaConnectorConfig config = connectorConfig(5_000, 1_000);
    TaskId task = tabletTask(0);
    OffsetReaderFixture offsets = new OffsetReaderFixture(Collections.emptyMap());

    ScyllaOffsetContext offsetContext =
        ScyllaOffsetContextLoader.load(config, tasks(task), offsets.reader, 10_000);

    assertTaskState(offsetContext.taskStateOffsetContext(task).getTaskState(), 5_000, 6_000);
    assertTrue(
        new ScyllaWorkerTransport(null, offsetContext, null, 0)
            .getTaskStatesForMigration(Set.of(legacyTask(task)))
            .isEmpty());
  }

  @Test
  void noOffsetAndNoLookbackLeavesInitializationToTheLibrary() {
    ScyllaConnectorConfig config = connectorConfig(0, 1_000);
    TaskId task = tabletTask(0);
    OffsetReaderFixture offsets = new OffsetReaderFixture(Collections.emptyMap());

    ScyllaOffsetContext offsetContext =
        ScyllaOffsetContextLoader.load(config, tasks(task), offsets.reader, 10_000);

    assertNull(offsetContext.taskStateOffsetContext(task).getTaskState());
    assertTrue(
        new ScyllaWorkerTransport(null, offsetContext, null, 0)
            .getTaskStatesForMigration(Set.of(legacyTask(task)))
            .isEmpty());
  }

  @Test
  void masterFallsBackToCompleteLegacyOffsetWhenAssignedOffsetIsAbsent() {
    ScyllaConnectorConfig config = connectorConfig(0, 1_000);
    TaskId task = tabletTask(0);
    TaskId legacyTask = legacyTask(task);
    OffsetReaderFixture offsets =
        new OffsetReaderFixture(
            Map.of(new SourceInfo(config, legacyTask).partition(), offset(7_000, 8_000)));
    SourceConnectorContext connectorContext = mock(SourceConnectorContext.class);
    when(connectorContext.offsetStorageReader()).thenReturn(offsets.reader);

    boolean fullyConsumed =
        new ScyllaMasterTransport(connectorContext, config)
            .areTasksFullyConsumedUntil(Set.of(task), new Timestamp(new Date(5_000)));

    assertTrue(fullyConsumed);
    assertEquals(
        Set.of(
            new SourceInfo(config, task).partition(),
            new SourceInfo(config, legacyTask).partition()),
        offsets.onlyRequestedPartitions());
  }

  @Test
  void masterDoesNotUseLegacyOffsetWhenAssignedOffsetIsPresent() {
    ScyllaConnectorConfig config = connectorConfig(0, 1_000);
    TaskId task = tabletTask(0);
    TaskId legacyTask = legacyTask(task);
    Map<Map<String, String>, Map<String, Object>> storedOffsets = new HashMap<>();
    storedOffsets.put(new SourceInfo(config, task).partition(), offset(4_000, 5_000));
    storedOffsets.put(new SourceInfo(config, legacyTask).partition(), offset(7_000, 8_000));
    OffsetReaderFixture offsets = new OffsetReaderFixture(storedOffsets);
    SourceConnectorContext connectorContext = mock(SourceConnectorContext.class);
    when(connectorContext.offsetStorageReader()).thenReturn(offsets.reader);

    boolean fullyConsumed =
        new ScyllaMasterTransport(connectorContext, config)
            .areTasksFullyConsumedUntil(Set.of(task), new Timestamp(new Date(5_000)));

    assertFalse(fullyConsumed);
  }

  @Test
  void masterTreatsMissingAssignedAndLegacyOffsetsAsIncomplete() {
    ScyllaConnectorConfig config = connectorConfig(0, 1_000);
    TaskId task = tabletTask(0);
    OffsetReaderFixture offsets = new OffsetReaderFixture(Collections.emptyMap());
    SourceConnectorContext connectorContext = mock(SourceConnectorContext.class);
    when(connectorContext.offsetStorageReader()).thenReturn(offsets.reader);

    boolean fullyConsumed =
        new ScyllaMasterTransport(connectorContext, config)
            .areTasksFullyConsumedUntil(Set.of(task), new Timestamp(new Date(5_000)));

    assertFalse(fullyConsumed);
  }

  private static ScyllaConnectorConfig connectorConfig(long lookbackMs, int queryWindowMs) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.table")
            .with(ScyllaConnectorConfig.INITIAL_LOOKBACK_MS.name(), lookbackMs)
            .with(ScyllaConnectorConfig.QUERY_TIME_WINDOW_SIZE.name(), queryWindowMs)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private static TaskId tabletTask(int streamIndex) {
    return TaskId.forTabletStream(GENERATION, streamIndex, TABLE);
  }

  private static TaskId legacyTask(TaskId task) {
    return TabletTaskOffsetMigration.legacyTaskId(task).orElseThrow();
  }

  private static List<Pair<TaskId, SortedSet<StreamId>>> tasks(TaskId... taskIds) {
    return Arrays.stream(taskIds)
        .map(taskId -> Pair.of(taskId, (SortedSet<StreamId>) new TreeSet<StreamId>()))
        .collect(Collectors.toList());
  }

  private static Map<String, Object> offset(long windowStartMs, long windowEndMs) {
    return Map.of(
        SourceInfo.WINDOW_START,
        UUIDs.startOf(windowStartMs).toString(),
        SourceInfo.WINDOW_END,
        UUIDs.endOf(windowEndMs).toString());
  }

  private static void assertTaskState(
      TaskState taskState, long expectedWindowStartMs, long expectedWindowEndMs) {
    assertEquals(expectedWindowStartMs, taskState.getWindowStartTimestamp().toDate().getTime());
    assertEquals(expectedWindowEndMs, taskState.getWindowEndTimestamp().toDate().getTime());
  }

  private static final class OffsetReaderFixture {
    private final OffsetStorageReader reader = mock(OffsetStorageReader.class);
    private final List<Collection<Map<String, String>>> requests = new ArrayList<>();

    private OffsetReaderFixture(Map<Map<String, String>, Map<String, Object>> storedOffsets) {
      doAnswer(
              invocation -> {
                Collection<Map<String, String>> partitions = invocation.getArgument(0);
                requests.add(new ArrayList<>(partitions));
                Map<Map<String, String>, Map<String, Object>> result = new HashMap<>();
                partitions.forEach(
                    partition -> {
                      Map<String, Object> offset = storedOffsets.get(partition);
                      if (offset != null) {
                        result.put(partition, offset);
                      }
                    });
                return result;
              })
          .when(reader)
          .offsets(anyCollection());
    }

    private Set<Map<String, String>> onlyRequestedPartitions() {
      assertEquals(1, requests.size());
      return new HashSet<>(requests.get(0));
    }
  }
}
