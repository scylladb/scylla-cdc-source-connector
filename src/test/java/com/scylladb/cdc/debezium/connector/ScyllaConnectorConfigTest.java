package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.NonFrozenCollectionEmptyRepresentation;
import io.debezium.config.Configuration;
import org.junit.jupiter.api.Test;

public class ScyllaConnectorConfigTest {

  private Configuration.Builder createMinimalConfigBuilder() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.table");
  }

  @Test
  void nonFrozenCollectionEmptyRepresentationDefaultIsNull() {
    ScyllaConnectorConfig config = new ScyllaConnectorConfig(createMinimalConfigBuilder().build());

    assertEquals(
        NonFrozenCollectionEmptyRepresentation.NULL,
        config.getNonFrozenCollectionEmptyRepresentation());
  }

  @Test
  void nonFrozenCollectionEmptyRepresentationCanBeSetToEmpty() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(
            createMinimalConfigBuilder()
                .with(
                    ScyllaConnectorConfig.CDC_NON_FROZEN_COLLECTION_EMPTY_REPRESENTATION_KEY,
                    "empty")
                .build());

    assertEquals(
        NonFrozenCollectionEmptyRepresentation.EMPTY,
        config.getNonFrozenCollectionEmptyRepresentation());
  }

  @Test
  void nonFrozenCollectionEmptyRepresentationParseDefaultsToNull() {
    assertEquals(NonFrozenCollectionEmptyRepresentation.NULL, parse(null));
    assertEquals(NonFrozenCollectionEmptyRepresentation.NULL, parse(""));
    assertEquals(NonFrozenCollectionEmptyRepresentation.NULL, parse("invalid"));
  }

  @Test
  void nonFrozenCollectionEmptyRepresentationParseIsCaseInsensitive() {
    assertEquals(NonFrozenCollectionEmptyRepresentation.NULL, parse("NULL"));
    assertEquals(NonFrozenCollectionEmptyRepresentation.EMPTY, parse(" Empty "));
  }

  private NonFrozenCollectionEmptyRepresentation parse(String value) {
    return NonFrozenCollectionEmptyRepresentation.parse(value);
  }
}
