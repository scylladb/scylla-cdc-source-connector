package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SharedSessionCache}.
 *
 * <p>These tests focus on the {@link SharedSessionCache.SessionKey} equality and hashing behavior,
 * which determines whether two connector configurations share the same CQL session. Full cache
 * lifecycle tests (acquire/release) require a live Scylla cluster and belong in integration tests.
 */
public class SharedSessionCacheTest {

  private ScyllaConnectorConfig createConfig(String contactPoints) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithAuth(
      String contactPoints, String user, String password) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("scylla.user", user)
            .with("scylla.password", password)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithLocalDC(String contactPoints, String localDC) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("scylla.local.dc", localDC)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithSsl(String contactPoints) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("scylla.ssl.enabled", "true")
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithSslCipherSuites(
      String contactPoints, String cipherSuites) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("scylla.ssl.enabled", "true")
            .with("scylla.ssl.cipherSuites", cipherSuites)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithFetchSize(String contactPoints, int fetchSize) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("scylla.query.options.fetch.size", fetchSize)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  private ScyllaConnectorConfig createConfigWithSharedSessionEnabled(
      String contactPoints, boolean enabled) {
    Configuration config =
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", contactPoints)
            .with("scylla.table.names", "ks.table")
            .with("worker.shared.session.enabled", enabled)
            .build();
    return new ScyllaConnectorConfig(config);
  }

  @Nested
  class FeatureFlagTests {

    @Test
    void sharedSessionEnabled_defaultsToFalse() {
      assertFalse(createConfig("127.0.0.1:9042").isSharedSessionEnabled());
    }

    @Test
    void sharedSessionEnabled_canBeEnabled() {
      assertTrue(
          createConfigWithSharedSessionEnabled("127.0.0.1:9042", true).isSharedSessionEnabled());
    }
  }

  @Nested
  class SessionKeyTests {

    @Test
    void identicalConfigs_produceSameKey() {
      ScyllaConnectorConfig config1 = createConfig("127.0.0.1:9042");
      ScyllaConnectorConfig config2 = createConfig("127.0.0.1:9042");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertEquals(key1, key2);
      assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void differentContactPoints_produceDifferentKeys() {
      ScyllaConnectorConfig config1 = createConfig("127.0.0.1:9042");
      ScyllaConnectorConfig config2 = createConfig("10.0.0.1:9042");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void contactPointOrder_doesNotAffectKey() {
      ScyllaConnectorConfig config1 = createConfig("10.0.0.1:9042,10.0.0.2:9042");
      ScyllaConnectorConfig config2 = createConfig("10.0.0.2:9042,10.0.0.1:9042");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertEquals(key1, key2);
      assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void differentCredentials_produceDifferentKeys() {
      ScyllaConnectorConfig config1 = createConfigWithAuth("127.0.0.1:9042", "user1", "pass1");
      ScyllaConnectorConfig config2 = createConfigWithAuth("127.0.0.1:9042", "user2", "pass2");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void sameCredentials_produceSameKey() {
      ScyllaConnectorConfig config1 = createConfigWithAuth("127.0.0.1:9042", "user1", "pass1");
      ScyllaConnectorConfig config2 = createConfigWithAuth("127.0.0.1:9042", "user1", "pass1");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertEquals(key1, key2);
      assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    void differentLocalDC_produceDifferentKeys() {
      ScyllaConnectorConfig config1 = createConfigWithLocalDC("127.0.0.1:9042", "dc1");
      ScyllaConnectorConfig config2 = createConfigWithLocalDC("127.0.0.1:9042", "dc2");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void sslEnabled_vs_disabled_produceDifferentKeys() {
      ScyllaConnectorConfig config1 = createConfig("127.0.0.1:9042");
      ScyllaConnectorConfig config2 = createConfigWithSsl("127.0.0.1:9042");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void cipherSuiteOrder_affectsKey() {
      ScyllaConnectorConfig config1 =
          createConfigWithSslCipherSuites(
              "127.0.0.1:9042", "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384");
      ScyllaConnectorConfig config2 =
          createConfigWithSslCipherSuites(
              "127.0.0.1:9042", "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256");

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void differentFetchSize_produceDifferentKeys() {
      ScyllaConnectorConfig config1 = createConfigWithFetchSize("127.0.0.1:9042", 1000);
      ScyllaConnectorConfig config2 = createConfigWithFetchSize("127.0.0.1:9042", 5000);

      SharedSessionCache.SessionKey key1 = SharedSessionCache.SessionKey.from(config1);
      SharedSessionCache.SessionKey key2 = SharedSessionCache.SessionKey.from(config2);

      assertNotEquals(key1, key2);
    }

    @Test
    void summary_containsUsefulInfo() {
      ScyllaConnectorConfig config = createConfigWithLocalDC("127.0.0.1:9042", "dc1");
      SharedSessionCache.SessionKey key = SharedSessionCache.SessionKey.from(config);

      String summary = key.summary();
      assertNotNull(summary);
      // Summary should contain contact points and DC info
      assertEquals(true, summary.contains("dc1"));
      assertEquals(true, summary.contains("ssl=false"));
    }

    @Test
    void poolingParams_produceDifferentKeys() {
      // Pool sizing is part of the effective session configuration, so different values should
      // not share the same cached session.
      Configuration config1 =
          Configuration.create()
              .with("name", "test-connector")
              .with("topic.prefix", "test")
              .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
              .with("scylla.table.names", "ks.table")
              .with("worker.pooling.core.pool.local", 1)
              .with("worker.pooling.max.pool.local", 2)
              .with("worker.pooling.max.requests.per.connection", 128)
              .with("worker.pooling.max.queue.size", 64)
              .with("worker.pooling.pool.timeout.ms", 1000)
              .build();
      Configuration config2 =
          Configuration.create()
              .with("name", "test-connector")
              .with("topic.prefix", "test")
              .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
              .with("scylla.table.names", "ks.table")
              .with("worker.pooling.core.pool.local", 4)
              .with("worker.pooling.max.pool.local", 8)
              .with("worker.pooling.max.requests.per.connection", 256)
              .with("worker.pooling.max.queue.size", 128)
              .with("worker.pooling.pool.timeout.ms", 5000)
              .build();

      SharedSessionCache.SessionKey key1 =
          SharedSessionCache.SessionKey.from(new ScyllaConnectorConfig(config1));
      SharedSessionCache.SessionKey key2 =
          SharedSessionCache.SessionKey.from(new ScyllaConnectorConfig(config2));

      assertNotEquals(key1, key2);
    }
  }

  @Nested
  class CacheStatsTests {

    @Test
    void getCacheStats_returnsValidStructure() {
      Map<String, Object> stats = SharedSessionCache.getCacheStats();
      assertNotNull(stats);
      assertNotNull(stats.get("totalSessions"));
      assertNotNull(stats.get("referenceCounts"));
    }
  }
}
