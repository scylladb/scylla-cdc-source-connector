package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import java.net.InetSocketAddress;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to detect the Scylla cluster version.
 *
 * <p>Connects to the cluster and retrieves the release version from the first available host.
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
}
