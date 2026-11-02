package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator;
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.scylladb.cdc.cql.CQLConfiguration.AddressTranslatorType;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to detect the Scylla cluster version.
 *
 * <p>Connects to the cluster and reads the ScyllaDB version of the coordinator node from {@code
 * system.versions}. The connection uses the connector's contact points, credentials, address
 * translator, and SSL/TLS settings.
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
    SSLOptions sslOptions = null;
    if (config.getSslEnabled()) {
      try {
        sslOptions = buildSslOptions();
      } catch (Exception e) {
        logger.warn("Failed to configure SSL for version check: {}", e.getMessage());
        return null;
      }
    }
    Cluster.Builder clusterBuilder;
    try {
      // After SSL: an address translator holds a DNS context that only the cluster closes.
      clusterBuilder = newClusterBuilder();
    } catch (Exception e) {
      // e.g. EC2MultiRegionAddressTranslator failing to create its DNS context
      logger.warn("Failed to configure the version check: {}", e.getMessage());
      return null;
    }
    if (sslOptions != null) {
      clusterBuilder.withSSL(sslOptions);
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
   * Builds the cluster with the connector's connection settings, except SSL. It connects only to
   * the contact points; the driver opens one connection per shard on each.
   */
  Cluster.Builder newClusterBuilder() throws UnknownHostException {
    Cluster.Builder clusterBuilder = Cluster.builder();
    Set<InetAddress> contactAddresses = new HashSet<>();
    for (InetSocketAddress contactPoint : config.getContactPoints()) {
      clusterBuilder.addContactPoint(contactPoint.getHostString()).withPort(contactPoint.getPort());
      contactAddresses.addAll(
          Arrays.asList(InetAddress.getAllByName(contactPoint.getHostString())));
    }
    if (config.getUser() != null && config.getPassword() != null) {
      clusterBuilder.withCredentials(config.getUser(), config.getPassword());
    }
    // Round-robin rather than DC-aware: a contact point outside the local DC would be ignored.
    clusterBuilder.withLoadBalancingPolicy(
        new HostFilterPolicy(
            new RoundRobinPolicy(),
            host -> contactAddresses.contains(host.getEndPoint().resolve().getAddress())));
    if (config.getAddressTranslator() == AddressTranslatorType.EC2_MULTI_REGION) {
      clusterBuilder.withAddressTranslator(new EC2MultiRegionAddressTranslator());
    }
    return clusterBuilder;
  }

  /**
   * Reads the ScyllaDB version through the given session.
   *
   * @return the parsed version, or null if the row is missing, cannot be parsed, or the role may
   *     not read it; other driver errors propagate
   */
  static ScyllaVersion queryVersion(Session session) {
    ResultSet resultSet;
    try {
      resultSet = session.execute(VERSION_QUERY);
    } catch (UnauthorizedException e) {
      // Unlike system.local, system.versions is not readable by every role.
      logger.warn(
          "Cannot read the Scylla version; GRANT SELECT ON system.versions to the connector's"
              + " role: {}",
          e.getMessage());
      return null;
    }
    Host host = resultSet.getExecutionInfo().getQueriedHost();
    Row row = resultSet.one();
    if (row == null) {
      logger.warn("No local row in system.versions on host {}", host);
      return null;
    }

    String rawVersion = row.getString("version");
    ScyllaVersion version = ScyllaVersion.parse(rawVersion);
    if (version == null) {
      logger.warn("Failed to parse Scylla version from: {} on host {}", rawVersion, host);
    } else {
      logger.info("Detected Scylla version: {} from host {}", rawVersion, host);
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
