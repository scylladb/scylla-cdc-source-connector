package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.transport.GroupedTasks;
import io.debezium.config.Configuration;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class ScyllaStreamingChangeEventSourceGroupingTest {

  private static final GenerationId GENERATION_ONE = generation(1_700_000_000_000L);
  private static final GenerationId GENERATION_TWO = generation(1_700_000_100_000L);
  private static final TableName TABLE_ONE = new TableName("ks", "one");
  private static final TableName TABLE_TWO = new TableName("ks", "two");

  @Test
  void separatesOneKafkaTaskAssignmentIntoGenerationHomogeneousGroups() {
    TaskId first = taskId(GENERATION_ONE, 1, TABLE_ONE);
    TaskId second = taskId(GENERATION_TWO, 2, TABLE_TWO);
    TaskId third = taskId(GENERATION_ONE, 3, TABLE_ONE);
    ScyllaTaskContext taskContext =
        new ScyllaTaskContext(
            configuration(), Arrays.asList(task(first, 1), task(second, 2), task(third, 3)));

    List<GroupedTasks> groups = ScyllaStreamingChangeEventSource.createGroupedTasks(taskContext);

    assertEquals(2, groups.size());
    assertEquals(GENERATION_ONE, groups.get(0).getGenerationId());
    assertEquals(Set.of(first, third), groups.get(0).getTaskIds());
    assertEquals(GENERATION_TWO, groups.get(1).getGenerationId());
    assertEquals(Collections.singleton(second), groups.get(1).getTaskIds());
    groups.forEach(
        group ->
            assertEquals(
                Collections.singleton(group.getGenerationId()),
                group.getTaskIds().stream()
                    .map(TaskId::getGenerationId)
                    .collect(Collectors.toSet())));
  }

  @Test
  void rejectsEmptyKafkaTaskAssignment() {
    ScyllaTaskContext taskContext = new ScyllaTaskContext(configuration(), Collections.emptyList());

    assertThrows(
        ConnectException.class,
        () -> ScyllaStreamingChangeEventSource.createGroupedTasks(taskContext));
  }

  private static Configuration configuration() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.one,ks.two")
        .build();
  }

  private static Pair<TaskId, SortedSet<StreamId>> task(TaskId taskId, long streamToken) {
    return Pair.of(taskId, new TreeSet<>(Collections.singleton(stream(streamToken))));
  }

  private static TaskId taskId(GenerationId generation, int index, TableName table) {
    return new TaskId(generation, new VNodeId(index), table);
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
