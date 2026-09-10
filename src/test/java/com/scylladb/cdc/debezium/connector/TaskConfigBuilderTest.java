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
        .map(ConfigSerializerUtil::deserializeTaskConfig)
        .collect(Collectors.toList());
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
