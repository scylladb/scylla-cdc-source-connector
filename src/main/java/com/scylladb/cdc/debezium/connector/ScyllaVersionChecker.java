package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.List;
import java.util.Set;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to detect the Scylla cluster version.
 *
 * <p>Connects to the cluster and retrieves the release version from the first available host. The
 * connection respects all connector configuration including SSL/TLS, local datacenter, and
 * credentials.
 */
public class ScyllaVersionChecker {
  private static final Logger logger = LoggerFactory.getLogger(ScyllaVersionChecker.class);

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

    try (Cluster cluster = clusterBuilder.build()) {
      cluster.init();
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      if (hosts.isEmpty()) {
        logger.warn("No hosts found in cluster metadata, cannot determine Scylla version");
        return null;
      }

      // Get version from the first host
      Host host = hosts.iterator().next();
      String releaseVersion = host.getCassandraVersion().toString();
      logger.info("Detected Scylla version: {} from host {}", releaseVersion, host.getAddress());

      ScyllaVersion version = ScyllaVersion.parse(releaseVersion);
      if (version == null) {
        logger.warn("Failed to parse Scylla version from: {}", releaseVersion);
      }
      return version;
    } catch (Exception e) {
      logger.warn("Failed to detect Scylla version: {}", e.getMessage());
      return null;
    }
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
