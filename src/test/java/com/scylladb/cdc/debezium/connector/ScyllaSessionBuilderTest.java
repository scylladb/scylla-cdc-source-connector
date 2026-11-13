package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockConstruction;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import io.debezium.config.Configuration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ScyllaSessionBuilderTest {
  private Configuration.Builder configuration() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.table");
  }

  @Test
  void omittedConsistencyUsesLocalQuorumForCdcReads() {
    assertSessionConsistency(
        configuration().build(), CQLConfiguration.ConsistencyLevel.LOCAL_QUORUM);
  }

  @ParameterizedTest
  @EnumSource(CQLConfiguration.ConsistencyLevel.class)
  void explicitConsistencyReachesCdcSession(CQLConfiguration.ConsistencyLevel consistency) {
    assertSessionConsistency(
        configuration().with("scylla.consistency.level", consistency.name()).build(), consistency);
  }

  private void assertSessionConsistency(
      Configuration configuration, CQLConfiguration.ConsistencyLevel expected) {
    AtomicReference<CQLConfiguration> sessionConfiguration = new AtomicReference<>();
    // Capture the real library configuration without opening a connection to ScyllaDB.
    try (var sessions =
        mockConstruction(
            Driver3Session.class,
            (session, context) ->
                sessionConfiguration.set((CQLConfiguration) context.arguments().get(0)))) {
      new ScyllaSessionBuilder(new ScyllaConnectorConfig(configuration)).build();
      assertEquals(1, sessions.constructed().size());
      assertEquals(expected, sessionConfiguration.get().getConsistencyLevel());
    }
  }
}
