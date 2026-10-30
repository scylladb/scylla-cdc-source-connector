package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.NonFrozenCollectionEmptyRepresentation;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.netty.handler.ssl.SslProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigValue;
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

  @Test
  void paddedEnumValuesValidateAndParse() {
    Configuration configuration =
        createMinimalConfigBuilder()
            .with("scylla.ssl.provider", " openssl ")
            .with("scylla.consistency.level", " LOCAL_ONE ")
            .build();

    Map<String, ConfigValue> results = configuration.validate(ScyllaConnectorConfig.EXPOSED_FIELDS);
    assertTrue(results.get("scylla.ssl.provider").errorMessages().isEmpty());
    assertTrue(results.get("scylla.consistency.level").errorMessages().isEmpty());

    ScyllaConnectorConfig config = new ScyllaConnectorConfig(configuration);
    assertEquals(SslProvider.OPENSSL, config.getSslProvider());
    assertEquals(CQLConfiguration.ConsistencyLevel.LOCAL_ONE, config.getConsistencyLevel());
  }

  @Test
  void extendedHeadersAreOffByDefault() {
    assertFalse(
        new ScyllaConnectorConfig(createMinimalConfigBuilder().build()).isExtendedHeadersEnabled());
    assertEquals(
        false, ScyllaConnectorConfig.configDef().defaultValues().get("extended.headers.enabled"));
  }

  @Test
  void enablingExtendedHeadersAddsTheDebeziumContextHeaders() {
    Configuration configuration =
        createMinimalConfigBuilder().with("extended.headers.enabled", true).build();
    assertTrue(new ScyllaConnectorConfig(configuration).isExtendedHeadersEnabled());

    // EventDispatcher appends these headers to every record when the option is on
    Map<String, Object> headers = new HashMap<>();
    new DebeziumHeaderProducer(new ScyllaTaskContext(configuration, List.of()))
        .contextHeaders()
        .forEach(header -> headers.put(header.key(), header.value()));
    assertEquals(
        Set.of(
            "__debezium.context.connectorLogicalName",
            "__debezium.context.taskId",
            "__debezium.context.connectorName",
            "__debezium.context.runId"),
        headers.keySet());
    assertEquals("test", headers.get("__debezium.context.connectorLogicalName"));
    assertEquals("0", headers.get("__debezium.context.taskId"));
  }
}
