package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.scylladb.cdc.cql.CQLConfiguration.AddressTranslatorType;
import io.debezium.config.Configuration;
import org.junit.jupiter.api.Test;

public class ScyllaAddressTranslatorConfigTest {
  private static Configuration.Builder baseConfig() {
    return Configuration.create()
        .with("name", "test-connector")
        .with("topic.prefix", "test")
        .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
        .with("scylla.table.names", "ks.table");
  }

  @Test
  public void testDefaultsToNone() {
    ScyllaConnectorConfig config = new ScyllaConnectorConfig(baseConfig().build());
    assertEquals(AddressTranslatorType.NONE, config.getAddressTranslator());
  }

  @Test
  public void testEc2MultiRegionCaseInsensitive() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(
            baseConfig().with("scylla.address.translator", "ec2_multi_region").build());
    assertEquals(AddressTranslatorType.EC2_MULTI_REGION, config.getAddressTranslator());
  }

  @Test
  public void testExplicitNone() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(baseConfig().with("scylla.address.translator", "NONE").build());
    assertEquals(AddressTranslatorType.NONE, config.getAddressTranslator());
  }

  @Test
  public void testInvalidValueFallsBackToDefault() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(baseConfig().with("scylla.address.translator", "bogus").build());
    assertEquals(AddressTranslatorType.NONE, config.getAddressTranslator());
  }
}
