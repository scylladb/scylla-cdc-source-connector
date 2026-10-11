package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.scylladb.cdc.cql.CQLConfiguration.AddressTranslatorType;
import io.debezium.config.Configuration;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

@Isolated
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
  public void testEc2MultiRegionTrimmed() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(
            baseConfig().with("scylla.address.translator", " EC2_MULTI_REGION ").build());
    assertEquals(AddressTranslatorType.EC2_MULTI_REGION, config.getAddressTranslator());
  }

  @Test
  public void testEc2MultiRegionWithTurkishLocale() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      ScyllaConnectorConfig config =
          new ScyllaConnectorConfig(
              baseConfig().with("scylla.address.translator", "ec2_multi_region").build());
      assertEquals(AddressTranslatorType.EC2_MULTI_REGION, config.getAddressTranslator());
      assertTrue(
          baseConfig()
              .with("scylla.address.translator", "ec2_multi_region")
              .build()
              .validate(ScyllaConnectorConfig.EXPOSED_FIELDS)
              .get(ScyllaConnectorConfig.ADDRESS_TRANSLATOR.name())
              .errorMessages()
              .isEmpty());
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  public void testExplicitNone() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(baseConfig().with("scylla.address.translator", "NONE").build());
    assertEquals(AddressTranslatorType.NONE, config.getAddressTranslator());
  }

  @Test
  public void testConnectValidationRejectsInvalidValue() {
    Configuration config = baseConfig().with("scylla.address.translator", "bogus").build();
    assertFalse(
        config
            .validate(ScyllaConnectorConfig.EXPOSED_FIELDS)
            .get(ScyllaConnectorConfig.ADDRESS_TRANSLATOR.name())
            .errorMessages()
            .isEmpty());
  }

  @Test
  public void testConnectValidationAcceptsTrimmedValue() {
    Configuration config =
        baseConfig().with("scylla.address.translator", " EC2_MULTI_REGION ").build();
    assertTrue(
        config
            .validate(ScyllaConnectorConfig.EXPOSED_FIELDS)
            .get(ScyllaConnectorConfig.ADDRESS_TRANSLATOR.name())
            .errorMessages()
            .isEmpty());
  }
}
