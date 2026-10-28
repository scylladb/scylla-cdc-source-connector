package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import io.debezium.config.Configuration;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ScyllaChangesConsumer construction and configuration constants. Record dispatch
 * and batch handling are covered by {@link ScyllaChangesConsumerBatchOffsetTest}.
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

  private Map<?, ?> getPendingBatches(ScyllaChangesConsumer consumer) throws Exception {
    Field field = ScyllaChangesConsumer.class.getDeclaredField("pendingBatches");
    field.setAccessible(true);
    return (Map<?, ?>) field.get(consumer);
  }

  @Test
  void constructor_startsWithNoPendingBatch_inEveryMode() throws Exception {
    for (CdcIncludeMode before : CdcIncludeMode.values()) {
      for (CdcIncludeMode after : CdcIncludeMode.values()) {
        ScyllaChangesConsumer consumer =
            new ScyllaChangesConsumer(null, null, null, null, createConfig(before, after), null);
        Map<?, ?> pendingBatches = getPendingBatches(consumer);
        assertTrue(pendingBatches.isEmpty(), before + "/" + after);
        assertEquals(ConcurrentHashMap.class, pendingBatches.getClass());
      }
    }
  }

  @Test
  void constructor_withCustomTimeout() throws Exception {
    ScyllaConnectorConfig config = createConfig(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(null, null, null, null, config, null, 1000);
    assertNotNull(getPendingBatches(consumer));
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
