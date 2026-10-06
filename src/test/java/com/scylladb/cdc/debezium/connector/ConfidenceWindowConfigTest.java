package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class ConfidenceWindowConfigTest {

  @Test
  void confidenceWindowMustBePositive() {
    assertFalse(validate("scylla.confidence.window.size", "0").errorMessages().isEmpty());
    assertFalse(validate("scylla.confidence.window.size", "-1").errorMessages().isEmpty());
    assertTrue(validate("scylla.confidence.window.size", "1").errorMessages().isEmpty());
  }

  @Test
  void queryTimeWindowMustBePositive() {
    assertFalse(validate("scylla.query.time.window.size", "0").errorMessages().isEmpty());
    assertFalse(validate("scylla.query.time.window.size", "-1").errorMessages().isEmpty());
    assertTrue(validate("scylla.query.time.window.size", "1").errorMessages().isEmpty());
  }

  private ConfigValue validate(String field, String value) {
    Configuration config = Configuration.create().with(field, value).build();
    return config.validate(ScyllaConnectorConfig.ALL_FIELDS).get(field);
  }
}
