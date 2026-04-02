package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3Session;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JVM-global cache of {@link Driver3Session} instances shared between Kafka Connect tasks that
 * target the same Scylla cluster with identical connection parameters.
 *
 * <p>Each Kafka Connect task previously created its own independent CQL session, leading to N
 * independent connection pools (where N = {@code tasks.max}) all targeting the same ScyllaDB nodes.
 * This caused aggregate in-flight request counts of {@code tasks.max * (max_requests_per_connection
 * + max_queue_size)} per node, which could overwhelm the cluster.
 *
 * <p>This cache ensures that tasks running in the same JVM with identical connection configurations
 * share a single underlying {@link Driver3Session}. Sessions are reference-counted: the first task
 * to {@link #acquire} a session creates it, subsequent tasks increment the reference count, and
 * {@link #release} decrements it. The session is closed only when the last task releases it.
 *
 * <p>When multiple tasks share a session, they also share the connection pool. This means the pool
 * may temporarily become busy under high concurrency. Callers should handle {@code
 * BusyPoolException} (wrapped in {@code NoHostAvailableException}) with retry and backoff rather
 * than treating it as a fatal error.
 *
 * <p>The session cache key is derived from connection-identity properties only (contact points,
 * port, credentials, SSL, consistency level, local DC, and fetch size). Pooling parameters are
 * intentionally excluded from the key so that tasks sharing the same cluster always share a session
 * regardless of per-task pooling configuration differences.
 *
 * <p>This class is thread-safe.
 */
public final class SharedSessionCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(SharedSessionCache.class);

  private static final ConcurrentHashMap<SessionKey, RefCountedSession> CACHE =
      new ConcurrentHashMap<>();

  private SharedSessionCache() {}

  /**
   * Acquires a shared {@link Driver3Session} for the given configuration. If an identical session
   * already exists in the cache, its reference count is incremented and the existing session is
   * returned. Otherwise, a new session is created and cached.
   *
   * @param configuration the connector configuration determining the session identity
   * @return a shared Driver3Session instance
   */
  public static Driver3Session acquire(ScyllaConnectorConfig configuration) {
    SessionKey key = SessionKey.from(configuration);
    AtomicReference<Exception> creationError = new AtomicReference<>();
    RefCountedSession refCounted =
        CACHE.compute(
            key,
            (k, existing) -> {
              if (existing != null && existing.retainIfOpen()) {
                LOGGER.info(
                    "Reusing shared CQL session for {} (ref count: {})",
                    k.summary(),
                    existing.refCount());
                return existing;
              }
              // existing is stale or null -- try to create new session
              try {
                LOGGER.info("Creating new shared CQL session for {}", k.summary());
                Driver3Session session = new ScyllaSessionBuilder(configuration).build();
                return new RefCountedSession(session);
              } catch (Exception e) {
                LOGGER.error("Failed to create shared CQL session for {}", k.summary(), e);
                // Return null to remove the stale entry, store exception for rethrow
                creationError.set(e);
                return null;
              }
            });
    if (creationError.get() != null) {
      throw new org.apache.kafka.connect.errors.ConnectException(
          "Failed to create CQL session", creationError.get());
    }
    return refCounted.session();
  }

  /**
   * Releases a previously acquired session. Decrements the reference count and closes the session
   * if no more tasks are using it.
   *
   * @param configuration the connector configuration that was used to acquire the session
   */
  public static void release(ScyllaConnectorConfig configuration) {
    SessionKey key = SessionKey.from(configuration);
    CACHE.compute(
        key,
        (k, existing) -> {
          if (existing == null) {
            LOGGER.warn("Attempted to release a session that is not in the cache: {}", k.summary());
            return null;
          }
          int remaining = existing.release();
          if (remaining < 0) {
            LOGGER.error(
                "Reference count went negative for {}: {} -- closing session and removing from cache",
                k.summary(),
                remaining);
            existing.close();
            return null; // Remove from cache to prevent zombie entry
          }
          if (remaining == 0) {
            LOGGER.info("Closing shared CQL session for {} (last reference released)", k.summary());
            existing.close();
            return null; // remove from map
          }
          LOGGER.info("Released shared CQL session for {} (ref count: {})", k.summary(), remaining);
          return existing;
        });
  }

  /** Returns the number of distinct sessions currently cached. Visible for testing. */
  static int size() {
    return CACHE.size();
  }

  /** Clears all cached sessions, closing them. Intended for testing only. */
  static void clearAll() {
    // Use computeIfPresent for atomic close-and-remove
    Set<SessionKey> keys = new HashSet<>(CACHE.keySet());
    keys.forEach(
        key -> {
          CACHE.computeIfPresent(
              key,
              (k, refCounted) -> {
                int refs = refCounted.refCount();
                if (refs > 0) {
                  LOGGER.warn(
                      "Force-closing shared CQL session for {} with {} active references",
                      k.summary(),
                      refs);
                } else {
                  LOGGER.info("Force-closing shared CQL session for {}", k.summary());
                }
                refCounted.close();
                return null; // Remove from map
              });
        });
  }

  /**
   * Returns cache state for debugging and monitoring. Visible for testing and monitoring.
   *
   * @return a map containing cache statistics including total sessions and reference counts
   */
  static Map<String, Object> getCacheStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("totalSessions", CACHE.size());

    Map<String, Integer> refCounts = new HashMap<>();
    CACHE.forEach(
        (key, refCounted) -> {
          refCounts.put(key.summary(), refCounted.refCount());
        });
    stats.put("referenceCounts", refCounts);

    return stats;
  }

  /**
   * A reference-counted wrapper around a {@link Driver3Session}. The session is closed only when
   * the reference count drops to zero.
   *
   * <p>Thread safety: All state-changing methods (retainIfOpen, release, close) must only be called
   * within {@code CACHE.compute()} which holds the ConcurrentHashMap bin lock, providing mutual
   * exclusion. The AtomicBoolean/AtomicInteger fields are used for their convenient CAS/atomic
   * operations, not for cross-thread visibility outside compute().
   */
  private static final class RefCountedSession {
    private final Driver3Session session;
    private final AtomicInteger refs;
    private final AtomicBoolean closed;

    RefCountedSession(Driver3Session session) {
      this.session = session;
      this.refs = new AtomicInteger(1);
      this.closed = new AtomicBoolean(false);
    }

    Driver3Session session() {
      return session;
    }

    /**
     * Retains the session only if it is not closed. Must be called within {@code CACHE.compute()}.
     *
     * @return true if the session was successfully retained, false if it was already closed
     */
    boolean retainIfOpen() {
      if (closed.get()) {
        return false;
      }
      refs.incrementAndGet();
      return true;
    }

    int release() {
      return refs.decrementAndGet();
    }

    int refCount() {
      return refs.get();
    }

    void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          session.close();
        } catch (Exception e) {
          LOGGER.error("Error closing shared CQL session - may leak resources", e);
        }
      }
    }
  }

  /**
   * Immutable key that identifies a unique CQL session configuration. Two tasks produce the same
   * key if and only if they connect to the same cluster with the same identity (credentials, SSL,
   * consistency) and query options (fetch size). Pooling parameters are intentionally excluded so
   * that tasks sharing the same cluster always share a session.
   */
  static final class SessionKey {
    private final String contactPoints;
    private final int defaultPort;
    private final String user;
    private final String password;
    private final String consistencyLevel;
    private final String localDC;
    private final boolean sslEnabled;
    private final String sslProvider;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String cipherSuites;
    private final String certPath;
    private final String privateKeyPath;
    private final int fetchSize;

    private SessionKey(
        String contactPoints,
        int defaultPort,
        String user,
        String password,
        String consistencyLevel,
        String localDC,
        boolean sslEnabled,
        String sslProvider,
        String trustStorePath,
        String trustStorePassword,
        String keyStorePath,
        String keyStorePassword,
        String cipherSuites,
        String certPath,
        String privateKeyPath,
        int fetchSize) {
      this.contactPoints = contactPoints;
      this.defaultPort = defaultPort;
      this.user = user;
      this.password = password;
      this.consistencyLevel = consistencyLevel;
      this.localDC = localDC;
      this.sslEnabled = sslEnabled;
      this.sslProvider = sslProvider;
      this.trustStorePath = trustStorePath;
      this.trustStorePassword = trustStorePassword;
      this.keyStorePath = keyStorePath;
      this.keyStorePassword = keyStorePassword;
      this.cipherSuites = cipherSuites;
      this.certPath = certPath;
      this.privateKeyPath = privateKeyPath;
      this.fetchSize = fetchSize;
    }

    static SessionKey from(ScyllaConnectorConfig config) {
      return new SessionKey(
          config.getContactPoints().stream()
              .map(InetSocketAddress::toString)
              .sorted()
              .collect(Collectors.joining(",")),
          config.getDefaultPort(),
          config.getUser(),
          config.getPassword(),
          config.getConsistencyLevel().name(),
          config.getLocalDCName(),
          config.getSslEnabled(),
          config.getSslEnabled()
              ? (config.getSslProvider() != null
                  ? config.getSslProvider().toString()
                  : SslProvider.JDK.toString())
              : null,
          config.getSslEnabled() ? config.getTrustStorePath() : null,
          config.getSslEnabled() ? config.getTrustStorePassword() : null,
          config.getSslEnabled() ? config.getKeyStorePath() : null,
          config.getSslEnabled() ? config.getKeyStorePassword() : null,
          config.getSslEnabled() && config.getCipherSuite() != null
              ? String.join(",", new TreeSet<>(config.getCipherSuite()))
              : null,
          config.getSslEnabled() ? config.getCertPath() : null,
          config.getSslEnabled() ? config.getPrivateKeyPath() : null,
          config.getQueryOptionsFetchSize());
    }

    /** Returns a short summary suitable for log messages (without sensitive data). */
    String summary() {
      return contactPoints + ":" + defaultPort + " (dc=" + localDC + ", ssl=" + sslEnabled + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SessionKey that = (SessionKey) o;
      return defaultPort == that.defaultPort
          && sslEnabled == that.sslEnabled
          && fetchSize == that.fetchSize
          && Objects.equals(contactPoints, that.contactPoints)
          && Objects.equals(user, that.user)
          && Objects.equals(password, that.password)
          && Objects.equals(consistencyLevel, that.consistencyLevel)
          && Objects.equals(localDC, that.localDC)
          && Objects.equals(sslProvider, that.sslProvider)
          && Objects.equals(trustStorePath, that.trustStorePath)
          && Objects.equals(trustStorePassword, that.trustStorePassword)
          && Objects.equals(keyStorePath, that.keyStorePath)
          && Objects.equals(keyStorePassword, that.keyStorePassword)
          && Objects.equals(cipherSuites, that.cipherSuites)
          && Objects.equals(certPath, that.certPath)
          && Objects.equals(privateKeyPath, that.privateKeyPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          contactPoints,
          defaultPort,
          user,
          password,
          consistencyLevel,
          localDC,
          sslEnabled,
          sslProvider,
          trustStorePath,
          trustStorePassword,
          keyStorePath,
          keyStorePassword,
          cipherSuites,
          certPath,
          privateKeyPath,
          fetchSize);
    }
  }
}
