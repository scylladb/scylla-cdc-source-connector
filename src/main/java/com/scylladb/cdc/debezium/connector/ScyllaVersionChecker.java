package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to detect the Scylla cluster version.
 *
 * <p>Connects to the cluster and reads the ScyllaDB version of the coordinator node from {@code
 * system.versions}. The connection respects all connector configuration including SSL/TLS, local
 * datacenter, and credentials.
 */
public class ScyllaVersionChecker {
  private static final Logger logger = LoggerFactory.getLogger(ScyllaVersionChecker.class);

  // system.local.release_version is the CQL-compatibility version (3.0.8 on every ScyllaDB
  // release), so the real version has to come from system.versions.
  static final String VERSION_QUERY = "SELECT version FROM system.versions WHERE key = 'local'";

  private final ScyllaConnectorConfig config;

  public ScyllaVersionChecker(ScyllaConnectorConfig config) {
    this.config = config;
  }

  /**
   * Gets the Scylla version from the cluster.
   *
   * @return the detected ScyllaVersion, or null if detection fails
   */
  public ScyllaVersion getVersion() {
    Cluster.Builder clusterBuilder = Cluster.builder();
    for (InetSocketAddress contactPoint : config.getContactPoints()) {
      clusterBuilder.addContactPoint(contactPoint.getHostString()).withPort(contactPoint.getPort());
    }
    if (config.getUser() != null && config.getPassword() != null) {
      clusterBuilder.withCredentials(config.getUser(), config.getPassword());
    }
    if (config.getLocalDCName() != null) {
      clusterBuilder.withLoadBalancingPolicy(
          DCAwareRoundRobinPolicy.builder().withLocalDc(config.getLocalDCName()).build());
    }
    if (config.getSslEnabled()) {
      try {
        clusterBuilder.withSSL(buildSslOptions());
      } catch (Exception e) {
        logger.warn("Failed to configure SSL for version check: {}", e.getMessage());
        return null;
      }
    }

    try (Cluster cluster = clusterBuilder.build();
        Session session = cluster.connect()) {
      return queryVersion(session);
    } catch (Exception e) {
      logger.warn("Failed to detect Scylla version: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Reads the ScyllaDB version through the given session.
   *
   * @return the parsed version, or null if the row is missing or cannot be parsed
   */
  static ScyllaVersion queryVersion(Session session) {
    Row row = session.execute(VERSION_QUERY).one();
    String rawVersion = row == null ? null : row.getString("version");
    logger.info("Detected Scylla version: {}", rawVersion);

    ScyllaVersion version = ScyllaVersion.parse(rawVersion);
    if (version == null) {
      logger.warn("Failed to parse Scylla version from: {}", rawVersion);
    }
    return version;
  }

  /** Builds SSL options from the connector configuration. */
  private SSLOptions buildSslOptions() throws Exception {
    SslProvider sslProvider = config.getSslProvider();
    if (sslProvider == null) {
      sslProvider = SslProvider.JDK;
    }

    return buildNettySslOptions(sslProvider);
  }

  private RemoteEndpointAwareNettySSLOptions buildNettySslOptions(SslProvider provider)
      throws Exception {
    SslContextBuilder contextBuilder = SslContextBuilder.forClient().sslProvider(provider);

    if (config.getTrustStorePath() != null) {
      KeyStore ts = createKeyStore(config.getTrustStorePath(), config.getTrustStorePassword());
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
      contextBuilder.trustManager(tmf);
    }

    if (config.getKeyStorePath() != null) {
      KeyStore ks = createKeyStore(config.getKeyStorePath(), config.getKeyStorePassword());
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      char[] ksPassword =
          config.getKeyStorePassword() != null ? config.getKeyStorePassword().toCharArray() : null;
      kmf.init(ks, ksPassword);
      contextBuilder.keyManager(kmf);
    }

    List<String> cipherSuites = config.getCipherSuite();
    if (cipherSuites != null && !cipherSuites.isEmpty()) {
      contextBuilder.ciphers(cipherSuites);
    }

    if (config.getCertPath() != null && config.getPrivateKeyPath() != null) {
      try (BufferedInputStream certInputStream =
              new BufferedInputStream(new FileInputStream(config.getCertPath()));
          BufferedInputStream privateKeyInputStream =
              new BufferedInputStream(new FileInputStream(config.getPrivateKeyPath()))) {
        contextBuilder.keyManager(certInputStream, privateKeyInputStream);
        SslContext sslContext = contextBuilder.build();
        return new RemoteEndpointAwareNettySSLOptions(sslContext);
      }
    } else if ((config.getCertPath() == null) != (config.getPrivateKeyPath() == null)) {
      throw new RuntimeException(
          String.format(
              "%s cannot be set without %s and vice-versa: %s is not set",
              "scylla.ssl.openssl.keyCertChain",
              "scylla.ssl.openssl.privateKey",
              (config.getCertPath() == null)
                  ? "scylla.ssl.openssl.keyCertChain"
                  : "scylla.ssl.openssl.privateKey"));
    }

    SslContext sslContext = contextBuilder.build();
    return new RemoteEndpointAwareNettySSLOptions(sslContext);
  }

  private KeyStore createKeyStore(String path, String password) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    char[] keyStorePassword = password != null ? password.toCharArray() : null;
    try (FileInputStream fis = new FileInputStream(path)) {
      keyStore.load(fis, keyStorePassword);
    }
    return keyStore;
  }
}
