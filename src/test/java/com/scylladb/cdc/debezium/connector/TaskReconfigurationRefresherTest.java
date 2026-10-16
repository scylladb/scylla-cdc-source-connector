package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class TaskReconfigurationRefresherTest {

  @Test
  void requestsReconfigurationRepeatedly() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    try (TaskReconfigurationRefresher ignored =
        new TaskReconfigurationRefresher(requests::incrementAndGet, 10)) {
      awaitAtLeast(requests, 3);
    }
  }

  @Test
  void keepsRequestingAfterAFailedRequest() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    Runnable failingFirst =
        () -> {
          if (requests.incrementAndGet() == 1) {
            throw new IllegalStateException("herder not ready");
          }
        };
    try (TaskReconfigurationRefresher ignored =
        new TaskReconfigurationRefresher(failingFirst, 10)) {
      awaitAtLeast(requests, 3);
    }
  }

  @Test
  void stopsRequestingAfterClose() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    TaskReconfigurationRefresher refresher =
        new TaskReconfigurationRefresher(requests::incrementAndGet, 10);
    awaitAtLeast(requests, 1);
    refresher.close();
    Thread.sleep(50);
    int afterClose = requests.get();
    Thread.sleep(100);
    assertEquals(afterClose, requests.get());
  }

  @Test
  void intervalDefaultsTo30sAllowsZeroAndRejectsTinyValues() {
    ScyllaConnectorConfig defaults = new ScyllaConnectorConfig(baseConfig().build());
    assertEquals(30000L, defaults.getTaskReconfigurationIntervalMs());
    assertTrue(validate("0").errorMessages().isEmpty());
    assertTrue(validate("1000").errorMessages().isEmpty());
    assertFalse(validate("999").errorMessages().isEmpty());
    assertFalse(validate("1").errorMessages().isEmpty());
    assertFalse(validate("-1").errorMessages().isEmpty());
    assertFalse(validate("abc").errorMessages().isEmpty());
  }

  private static Configuration.Builder baseConfig() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.table");
  }

  private static ConfigValue validate(String value) {
    String field = "scylla.task.reconfiguration.interval.ms";
    Configuration config = Configuration.create().with(field, value).build();
    return config.validate(ScyllaConnectorConfig.ALL_FIELDS).get(field);
  }

  private static void awaitAtLeast(AtomicInteger counter, int expected) throws Exception {
    long deadline = System.currentTimeMillis() + 5000;
    while (counter.get() < expected) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("expected >= " + expected + " requests, got " + counter.get());
      }
      Thread.sleep(5);
    }
  }
}
