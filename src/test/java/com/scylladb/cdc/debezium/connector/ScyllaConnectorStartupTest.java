package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Isolated
class ScyllaConnectorStartupTest {
  private static final String FLOGGER_BACKEND = System.getProperty("flogger.backend_factory");

  @AfterAll
  static void restoreFloggerBackend() {
    if (FLOGGER_BACKEND == null) {
      System.clearProperty("flogger.backend_factory");
    } else {
      System.setProperty("flogger.backend_factory", FLOGGER_BACKEND);
    }
  }

  @Test
  void missingLocalDcWarnsBeforeOpeningConnections() {
    List<String> warnings = startupWarnings(Map.of());
    assertEquals(1, warnings.size());
    String message = warnings.get(0);
    assertTrue(message.contains("coordinated in any datacenter"));
    assertTrue(message.contains("coordinator's datacenter"));
    assertTrue(message.contains("base-table LOCAL_QUORUM writes"));
    assertTrue(message.contains("scylla.consistency.level=QUORUM"));
  }

  @Test
  void configuredLocalDcSuppressesWarning() {
    assertTrue(startupWarnings(Map.of("scylla.local.dc", "dc1")).isEmpty());
  }

  private List<String> startupWarnings(Map<String, String> overrides) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", "test-connector");
    properties.put("topic.prefix", "test");
    properties.put("scylla.cluster.ip.addresses", "127.0.0.1:9042");
    properties.put("scylla.table.names", "ks.table");
    properties.putAll(overrides);
    Logger logger = mock(Logger.class);
    ScyllaConnector connector;
    try (var loggers = mockStatic(LoggerFactory.class)) {
      loggers.when(() -> LoggerFactory.getLogger(ScyllaConnector.class)).thenReturn(logger);
      connector = new ScyllaConnector();
    }
    RuntimeException stopBeforeConnection = new RuntimeException("stop before version probe");
    try (var probes =
        mockConstruction(
            ScyllaVersionChecker.class,
            (probe, context) -> when(probe.getVersion()).thenThrow(stopBeforeConnection))) {
      assertSame(
          stopBeforeConnection,
          assertThrows(RuntimeException.class, () -> connector.start(properties)));
      assertEquals(1, probes.constructed().size());
      return mockingDetails(logger).getInvocations().stream()
          .filter(invocation -> invocation.getMethod().getName().equals("warn"))
          .map(invocation -> invocation.<String>getArgument(0))
          .filter(message -> message.startsWith("scylla.local.dc is unset:"))
          .toList();
    }
  }
}
