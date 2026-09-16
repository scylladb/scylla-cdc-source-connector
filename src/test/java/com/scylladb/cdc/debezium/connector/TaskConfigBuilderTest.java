package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.transport.CoordinationGroup;
import com.scylladb.cdc.transport.CoordinationNamespaces;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class TaskConfigBuilderTest {

  private static final GenerationId GENERATION_ONE = generation(1_700_000_000_000L);
  private static final GenerationId GENERATION_TWO = generation(1_700_000_100_000L);

  @Test
  void keepsMixedGenerationsInOneConfigWhenMaxTasksIsOne() {
    Map<TaskId, SortedSet<StreamId>> tasks = mixedGenerationTasks();

    List<String> configs = new TaskConfigBuilder(tasks).buildTaskConfigs(1);

    assertEquals(1, configs.size());
    assertEquals(Set.of(GENERATION_ONE, GENERATION_TWO), generations(configs.get(0)));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void capsConfigsAtTwoWithoutDroppingMixedGenerationAssignments() {
    Map<TaskId, SortedSet<StreamId>> tasks = mixedGenerationTasks();

    List<String> configs = new TaskConfigBuilder(tasks).buildTaskConfigs(2);

    assertEquals(2, configs.size());
    assertTrue(configs.stream().anyMatch(config -> generations(config).size() > 1));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void usesEveryAvailableTaskConfigAtPartitionBoundary() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTasks(tasks, GENERATION_ONE, new TableName("ks", "large_table"), 101, 0);

    List<String> configs = new TaskConfigBuilder(tasks).buildTaskConfigs(100);

    assertEquals(100, configs.size());
    assertEquals(101, configs.stream().mapToInt(config -> deserialize(config).size()).sum());
    assertEquals(
        2, configs.stream().mapToInt(config -> deserialize(config).size()).max().getAsInt());
    assertEquals(
        1, configs.stream().mapToInt(config -> deserialize(config).size()).min().getAsInt());
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void balancesSerializedBytesWhenAssignmentLengthsDiffer() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    TableName longTable = new TableName("ks", "l".repeat(48));
    TableName shortTable = new TableName("ks", "s");
    addTasks(tasks, GENERATION_ONE, longTable, 1, 0);
    addTasks(tasks, GENERATION_ONE, shortTable, 1, 1);
    addTasks(tasks, GENERATION_ONE, longTable, 1, 2);
    addTasks(tasks, GENERATION_ONE, shortTable, 1, 3);

    List<String> configs = new TaskConfigBuilder(tasks, 180).buildTaskConfigs(2);

    assertEquals(2, configs.size());
    assertTrue(
        configs.stream().allMatch(config -> config.getBytes(StandardCharsets.UTF_8).length <= 180));
    assertTrue(configs.stream().allMatch(config -> deserialize(config).size() == 2));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void retriesWithCapAwarePackingWhenLeastLoadedPlacementExceedsLimit() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTask(tasks, GENERATION_ONE, new TableName("ks", "s"), 0, 8);
    addTask(tasks, GENERATION_ONE, new TableName("ks", "s"), 1, 7);
    addTask(tasks, GENERATION_ONE, new TableName("ks", "s"), 2, 6);
    addTask(tasks, GENERATION_ONE, new TableName("ks", "s"), 3, 5);
    addTask(tasks, GENERATION_ONE, new TableName("ks", "s"), 4, 4);

    List<String> configs = new TaskConfigBuilder(tasks, 600).buildTaskConfigs(2);

    assertEquals(2, configs.size());
    assertTrue(
        configs.stream().allMatch(config -> config.getBytes(StandardCharsets.UTF_8).length <= 600));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void rejectsOversizedWorkerConfigAndSuggestsScalingOptions() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTasks(tasks, GENERATION_ONE, new TableName("ks", "large_table"), 3, 0);

    ConnectException exception =
        assertThrows(
            ConnectException.class, () -> new TaskConfigBuilder(tasks, 100).buildTaskConfigs(1));

    assertTrue(exception.getMessage().contains("Increase tasks.max"));
    assertTrue(exception.getMessage().contains("split the configured tables"));
  }

  @Test
  void splitsHighCardinalityAssignmentBelowDefaultByteLimit() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTasks(tasks, GENERATION_ONE, new TableName("ks", "large_table"), 12_000, 0);

    assertThrows(ConnectException.class, () -> new TaskConfigBuilder(tasks).buildTaskConfigs(1));

    List<String> configs = new TaskConfigBuilder(tasks).buildTaskConfigs(2);
    assertEquals(2, configs.size());
    assertEquals(12_000, configs.stream().mapToInt(config -> deserialize(config).size()).sum());
  }

  @Test
  void copiesFullCoordinationManifestToEveryParticipatingWorkerConfig() {
    TableName table = new TableName("ks", "tablets");
    TaskId first = TaskId.forTabletStream(GENERATION_ONE, 0, table);
    TaskId second = TaskId.forTabletStream(GENERATION_ONE, 1, table);
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    tasks.put(first, new TreeSet<>(Collections.singleton(stream(1))));
    tasks.put(second, new TreeSet<>(Collections.singleton(stream(2))));
    CoordinationGroup<TaskId, TaskId> migration =
        new CoordinationGroup<>(
            CoordinationNamespaces.TABLET_TASK_STATE_MIGRATION,
            TaskId.legacyTabletTask(GENERATION_ONE, table),
            Set.of(first, second));

    List<String> configs =
        new TaskConfigBuilder(tasks, Set.of(migration), 2_048).buildTaskConfigs(2);

    assertEquals(2, configs.size());
    configs.forEach(
        config -> {
          assertEquals(1, deserialize(config).size());
          assertEquals(Set.of(migration), coordinationGroups(config));
        });
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void dropsCoordinationManifestThatDoesNotFitBesideItsTasks() {
    TableName table = new TableName("ks", "tablets");
    TaskId task = TaskId.forTabletStream(GENERATION_ONE, 0, table);
    Map<TaskId, SortedSet<StreamId>> tasks =
        Map.of(task, new TreeSet<>(Collections.singleton(stream(1))));
    CoordinationGroup<TaskId, TaskId> migration =
        new CoordinationGroup<>(
            CoordinationNamespaces.TABLET_TASK_STATE_MIGRATION,
            TaskId.legacyTabletTask(GENERATION_ONE, table),
            Set.of(task));
    String config =
        new TaskConfigBuilder(
                tasks, Set.of(migration), ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES)
            .buildTaskConfigs(1)
            .get(0);
    int serializedBytes = config.getBytes(StandardCharsets.UTF_8).length;

    // The byte limit covers the tasks and the manifest together, so one byte less no longer fits
    // both. The assignment is kept and the manifest, which only retires legacy checkpoints
    // earlier, is given up.
    List<String> configs =
        new TaskConfigBuilder(tasks, Set.of(migration), serializedBytes - 1).buildTaskConfigs(1);

    assertEquals(1, configs.size());
    assertEquals(Set.of(), coordinationGroups(configs.get(0)));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void keepsFittingCoordinationManifestsWhenDroppingAnOversizedOne() {
    TableName oversizedTable = new TableName("ks", "tablets_big");
    TableName smallTable = new TableName("ks", "tablets_small");
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTabletTasks(tasks, GENERATION_ONE, oversizedTable, 600);
    addTabletTasks(tasks, GENERATION_ONE, smallTable, 2);
    CoordinationGroup<TaskId, TaskId> oversizedMigration =
        migration(oversizedTable, tasks.keySet());
    CoordinationGroup<TaskId, TaskId> smallMigration = migration(smallTable, tasks.keySet());

    List<String> configs =
        new TaskConfigBuilder(tasks, Set.of(oversizedMigration, smallMigration), 4_096)
            .buildTaskConfigs(16);

    assertEquals(16, configs.size());
    assertTrue(
        configs.stream()
            .allMatch(config -> config.getBytes(StandardCharsets.UTF_8).length <= 4_096));
    assertEquals(
        Set.of(smallMigration),
        configs.stream()
            .flatMap(config -> coordinationGroups(config).stream())
            .collect(Collectors.toSet()));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void rejectsSingleAssignmentLargerThanTheByteLimit() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTask(tasks, GENERATION_ONE, new TableName("ks", "wide"), 0, 8);
    int assignmentBytes =
        ConfigSerializerUtil.serializeTaskConfig(
                tasks.keySet().iterator().next(), tasks.values().iterator().next())
            .getBytes(StandardCharsets.UTF_8)
            .length;

    ConnectException exception =
        assertThrows(
            ConnectException.class,
            () -> new TaskConfigBuilder(tasks, assignmentBytes - 1).buildTaskConfigs(4));

    assertTrue(exception.getMessage().contains("neither tasks.max nor splitting"));
  }

  @Test
  void preservesCoordinationGroupAffinityWhenPackingHighCardinalityTables() {
    TableName firstTable = new TableName("ks", "tablets_01");
    TableName secondTable = new TableName("ks", "tablets_02");
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTasks(tasks, GENERATION_ONE, firstTable, 10_500, 0);
    addTasks(tasks, GENERATION_ONE, secondTable, 10_501, 0);
    CoordinationGroup<TaskId, TaskId> firstMigration = migration(firstTable, tasks.keySet());
    CoordinationGroup<TaskId, TaskId> secondMigration = migration(secondTable, tasks.keySet());

    List<String> configs =
        new TaskConfigBuilder(
                tasks,
                Set.of(firstMigration, secondMigration),
                ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES)
            .buildTaskConfigs(2);

    assertEquals(2, configs.size());
    assertTrue(
        configs.stream()
            .allMatch(
                config ->
                    config.getBytes(StandardCharsets.UTF_8).length
                        <= ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES));
    assertEquals(
        Set.of(firstTable, secondTable),
        configs.stream()
            .flatMap(config -> coordinationGroups(config).stream())
            .map(group -> group.getKey().getTable())
            .collect(Collectors.toSet()));
    configs.forEach(
        config -> {
          Set<TableName> manifestTables =
              coordinationGroups(config).stream()
                  .map(group -> group.getKey().getTable())
                  .collect(Collectors.toSet());
          Set<TableName> taskTables =
              deserialize(config).stream()
                  .map(task -> task.getKey().getTable())
                  .collect(Collectors.toSet());
          assertEquals(1, manifestTables.size());
          assertEquals(manifestTables, taskTables);
        });
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void spreadsTabletTasksAcrossConfigsWhileEveryManifestStillFits() {
    TableName smallTable = new TableName("ks", "tablets_01");
    TableName largeTable = new TableName("ks", "tablets_02");
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTabletTasks(tasks, GENERATION_ONE, smallTable, 2);
    addTabletTasks(tasks, GENERATION_ONE, largeTable, 10);
    CoordinationGroup<TaskId, TaskId> smallMigration = migration(smallTable, tasks.keySet());
    CoordinationGroup<TaskId, TaskId> largeMigration = migration(largeTable, tasks.keySet());

    List<String> configs =
        new TaskConfigBuilder(
                tasks,
                Set.of(smallMigration, largeMigration),
                ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES)
            .buildTaskConfigs(4);

    // Parallelism comes first while the manifests fit: a small table is not confined to its own
    // share of the configurations, even though mixing tables copies both manifests into one
    // configuration. Affinity is only traded in when the byte ceiling forces it, as in
    // preservesCoordinationGroupAffinityWhenPackingHighCardinalityTables.
    assertEquals(4, configs.size());
    assertTrue(
        configs.stream()
            .anyMatch(
                config ->
                    deserialize(config).stream()
                            .map(task -> task.getKey().getTable())
                            .collect(Collectors.toSet())
                            .size()
                        == 2),
        "no configuration mixes the two tables");
    assertEquals(
        Set.of(smallMigration, largeMigration),
        configs.stream()
            .flatMap(config -> coordinationGroups(config).stream())
            .collect(Collectors.toSet()));
    assertConfigsRoundTrip(tasks, configs);
  }

  @Test
  void retriesWithLargestCoordinationGroupFirstBeforeRejectingValidLayout() {
    TableName firstTable = new TableName("ks", "a".repeat(39));
    TableName secondTable = new TableName("ks", "b".repeat(28));
    TableName largestTable = new TableName("ks", "c".repeat(20));
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTabletTasks(tasks, GENERATION_ONE, firstTable, 1_143);
    addTabletTasks(tasks, GENERATION_ONE, secondTable, 6_529);
    addTabletTasks(tasks, GENERATION_ONE, largestTable, 8_705);
    Set<CoordinationGroup<TaskId, TaskId>> migrations =
        Set.of(
            migration(firstTable, tasks.keySet()),
            migration(secondTable, tasks.keySet()),
            migration(largestTable, tasks.keySet()));

    List<String> configs =
        new TaskConfigBuilder(
                tasks, migrations, ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES)
            .buildTaskConfigs(2);

    assertEquals(2, configs.size());
    assertTrue(
        configs.stream()
            .allMatch(
                config ->
                    config.getBytes(StandardCharsets.UTF_8).length
                        <= ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES));
    assertConfigsRoundTrip(tasks, configs);
  }

  private static Map<TaskId, SortedSet<StreamId>> mixedGenerationTasks() {
    Map<TaskId, SortedSet<StreamId>> tasks = new LinkedHashMap<>();
    addTasks(tasks, GENERATION_ONE, new TableName("ks", "one"), 8, 0);
    addTasks(tasks, GENERATION_TWO, new TableName("ks", "two"), 4, 8);
    return tasks;
  }

  private static void addTasks(
      Map<TaskId, SortedSet<StreamId>> tasks,
      GenerationId generation,
      TableName table,
      int count,
      int firstIndex) {
    for (int offset = 0; offset < count; offset++) {
      int index = firstIndex + offset;
      tasks.put(
          new TaskId(generation, new VNodeId(index), table),
          new TreeSet<>(Collections.singleton(stream(index + 1L))));
    }
  }

  private static void addTask(
      Map<TaskId, SortedSet<StreamId>> tasks,
      GenerationId generation,
      TableName table,
      int index,
      int streamCount) {
    SortedSet<StreamId> streams = new TreeSet<>();
    for (int stream = 0; stream < streamCount; stream++) {
      streams.add(stream(index * 100L + stream + 1));
    }
    tasks.put(new TaskId(generation, new VNodeId(index), table), streams);
  }

  private static void addTabletTasks(
      Map<TaskId, SortedSet<StreamId>> tasks, GenerationId generation, TableName table, int count) {
    for (int index = 0; index < count; index++) {
      tasks.put(
          TaskId.forTabletStream(generation, index, table),
          new TreeSet<>(Collections.singleton(stream(tasks.size() + 1L))));
    }
  }

  private static CoordinationGroup<TaskId, TaskId> migration(TableName table, Set<TaskId> taskIds) {
    return new CoordinationGroup<>(
        CoordinationNamespaces.TABLET_TASK_STATE_MIGRATION,
        TaskId.legacyTabletTask(GENERATION_ONE, table),
        taskIds.stream()
            .filter(taskId -> taskId.getTable().equals(table))
            .collect(Collectors.toSet()));
  }

  private static Set<GenerationId> generations(String config) {
    return deserialize(config).stream()
        .map(task -> task.getKey().getGenerationId())
        .collect(Collectors.toSet());
  }

  private static void assertConfigsRoundTrip(
      Map<TaskId, SortedSet<StreamId>> expected, List<String> configs) {
    Map<TaskId, SortedSet<StreamId>> actual = new HashMap<>();
    configs.stream()
        .flatMap(config -> deserialize(config).stream())
        .forEach(task -> actual.put(task.getKey(), task.getValue()));
    assertEquals(expected, actual);
  }

  private static List<Pair<TaskId, SortedSet<StreamId>>> deserialize(String config) {
    return Arrays.stream(config.split("\n"))
        .filter(line -> !ConfigSerializerUtil.isSerializedCoordinationGroup(line))
        .map(ConfigSerializerUtil::deserializeTaskConfig)
        .collect(Collectors.toList());
  }

  private static Set<CoordinationGroup<TaskId, TaskId>> coordinationGroups(String config) {
    return Arrays.stream(config.split("\n"))
        .filter(ConfigSerializerUtil::isSerializedCoordinationGroup)
        .map(ConfigSerializerUtil::deserializeCoordinationGroup)
        .collect(Collectors.toSet());
  }

  private static GenerationId generation(long timestamp) {
    return new GenerationId(new Timestamp(new Date(timestamp)));
  }

  private static StreamId stream(long token) {
    ByteBuffer value = ByteBuffer.allocate(16);
    value.putLong(token);
    value.putLong(1L);
    value.flip();
    return new StreamId(value);
  }
}
