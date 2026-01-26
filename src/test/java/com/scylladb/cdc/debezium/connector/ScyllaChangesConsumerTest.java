package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.TaskId;
import io.debezium.config.Configuration;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ScyllaChangesConsumer.
 *
 * <p>These tests focus on the taskInfoMap management, stale task cleanup, and configuration
 * constants.
 */
public class ScyllaChangesConsumerTest {

  private ScyllaConnectorConfig createConfig(CdcIncludeMode before, CdcIncludeMode after) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.table")
            .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, before.getValue())
            .with(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY, after.getValue())
            .build();
    return new ScyllaConnectorConfig(config);
  }

  @SuppressWarnings("unchecked")
  private Map<TaskId, TaskInfo> getTaskInfoMap(ScyllaChangesConsumer consumer) throws Exception {
    Field field = ScyllaChangesConsumer.class.getDeclaredField("taskInfoMap");
    field.setAccessible(true);
    return (Map<TaskId, TaskInfo>) field.get(consumer);
  }

  @Test
  void constructor_createsTaskInfoMap_whenPreimagesEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void constructor_createsTaskInfoMap_whenPostimagesEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.NONE, CdcIncludeMode.FULL);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void constructor_createsTaskInfoMap_whenBothEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.FULL);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void constructor_noTaskInfoMap_whenBothDisabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNull(taskInfoMap);
  }

  @Test
  void getOrCreateTaskInfo_throwsWhenMapIsNull() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    // Access the private method via reflection
    Method method =
        ScyllaChangesConsumer.class.getDeclaredMethod("getOrCreateTaskInfo", TaskId.class);
    method.setAccessible(true);

    // Should throw IllegalStateException
    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              try {
                method.invoke(consumer, (TaskId) null);
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            });

    assertEquals(IllegalStateException.class, exception.getClass());
    assertTrue(exception.getMessage().contains("taskInfoMap is null"));
  }

  @Test
  void cleanupStaleTasks_doesNothingWhenMapIsNull() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    // Access the private method via reflection
    Method cleanupMethod = ScyllaChangesConsumer.class.getDeclaredMethod("cleanupStaleTasks");
    cleanupMethod.setAccessible(true);

    // Should not throw when map is null
    cleanupMethod.invoke(consumer);
  }

  @Test
  void cleanupStaleTasks_doesNothingWhenMapIsEmpty() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    // Access the private method via reflection
    Method cleanupMethod = ScyllaChangesConsumer.class.getDeclaredMethod("cleanupStaleTasks");
    cleanupMethod.setAccessible(true);

    // Should not throw when map is empty
    cleanupMethod.invoke(consumer);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void configKeyConstants_areCorrect() {
    assertEquals("cdc.include.before", ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY);
    assertEquals("cdc.include.after", ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY);
  }

  @Test
  void configKeyConstants_matchFieldNames() {
    // Verify the constants are used in the Field definitions
    assertEquals(
        ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY,
        ScyllaConnectorConfig.CDC_INCLUDE_BEFORE.name());
    assertEquals(
        ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY,
        ScyllaConnectorConfig.CDC_INCLUDE_AFTER.name());
  }

  @Test
  void constructor_withCustomTimeout() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
    long customTimeoutMs = 1000;

    // Use the package-private constructor with custom timeout
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(null, null, null, null, config, customTimeoutMs);

    // Verify the consumer was created successfully
    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
  }

  @Test
  void taskInfoMapIsConcurrentHashMap_whenEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);

    // Verify it's a ConcurrentHashMap
    assertEquals("java.util.concurrent.ConcurrentHashMap", taskInfoMap.getClass().getName());
  }

  @Test
  void constructor_createsTaskInfoMap_whenOnlyUpdatedBeforeEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void constructor_createsTaskInfoMap_whenOnlyUpdatedAfterEnabled() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.NONE, CdcIncludeMode.ONLY_UPDATED);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void constructor_createsTaskInfoMap_whenOnlyUpdatedBothEnabled() throws Exception {
    ScyllaConnectorConfig config =
        createConfig(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.ONLY_UPDATED);
    ScyllaChangesConsumer consumer = new ScyllaChangesConsumer(null, null, null, null, config);

    Map<TaskId, TaskInfo> taskInfoMap = getTaskInfoMap(consumer);
    assertNotNull(taskInfoMap);
    assertEquals(0, taskInfoMap.size());
  }

  @Test
  void cdcIncludeMode_requiresImage_returnsCorrectValues() {
    assertTrue(CdcIncludeMode.FULL.requiresImage());
    assertTrue(CdcIncludeMode.ONLY_UPDATED.requiresImage());
    assertEquals(false, CdcIncludeMode.NONE.requiresImage());
  }

  @Test
  void cdcIncludeMode_parse_handlesOnlyUpdated() {
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("only-updated"));
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("ONLY-UPDATED"));
    assertEquals(CdcIncludeMode.ONLY_UPDATED, CdcIncludeMode.parse("  only-updated  "));
  }
}
