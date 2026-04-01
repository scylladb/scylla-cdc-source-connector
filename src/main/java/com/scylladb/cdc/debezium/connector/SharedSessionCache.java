package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3Session;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>Tasks targeting different clusters, using different credentials, or configured with different
 * pooling/SSL/consistency parameters will get separate sessions, since the cache key is derived
 * from all connection-relevant configuration properties.
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
    RefCountedSession refCounted =
        CACHE.compute(
            key,
            (k, existing) -> {
              if (existing != null && !existing.isClosed()) {
                existing.retain();
                LOGGER.info(
                    "Reusing shared CQL session for {} (ref count: {})",
                    k.summary(),
                    existing.refCount());
                return existing;
              }
              LOGGER.info("Creating new shared CQL session for {}", k.summary());
              Driver3Session session = new ScyllaSessionBuilder(configuration).build();
              return new RefCountedSession(session);
            });
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
          if (remaining <= 0) {
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
    CACHE.forEach(
        (key, refCounted) -> {
          LOGGER.info("Force-closing shared CQL session for {}", key.summary());
          refCounted.close();
        });
    CACHE.clear();
  }

  /**
   * A reference-counted wrapper around a {@link Driver3Session}. The session is closed only when
   * the reference count drops to zero.
   */
  private static final class RefCountedSession {
    private final Driver3Session session;
    private final AtomicInteger refs;
    private volatile boolean closed;

    RefCountedSession(Driver3Session session) {
      this.session = session;
      this.refs = new AtomicInteger(1);
      this.closed = false;
    }

    Driver3Session session() {
      return session;
    }

    void retain() {
      refs.incrementAndGet();
    }

    int release() {
      return refs.decrementAndGet();
    }

    int refCount() {
      return refs.get();
    }

    boolean isClosed() {
      return closed;
    }

    void close() {
      if (!closed) {
        closed = true;
        try {
          session.close();
        } catch (Exception e) {
          LOGGER.warn("Error closing shared CQL session", e);
        }
      }
    }
  }

  /**
   * Immutable key that identifies a unique CQL session configuration. Two tasks produce the same
   * key if and only if they would create functionally identical {@link Driver3Session} instances
   * (same cluster, credentials, SSL, pooling, consistency, etc.).
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
    private final int corePoolLocal;
    private final int maxPoolLocal;
    private final int maxRequestsPerConnection;
    private final int maxQueueSize;
    private final int poolTimeoutMs;

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
        int fetchSize,
        int corePoolLocal,
        int maxPoolLocal,
        int maxRequestsPerConnection,
        int maxQueueSize,
        int poolTimeoutMs) {
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
      this.corePoolLocal = corePoolLocal;
      this.maxPoolLocal = maxPoolLocal;
      this.maxRequestsPerConnection = maxRequestsPerConnection;
      this.maxQueueSize = maxQueueSize;
      this.poolTimeoutMs = poolTimeoutMs;
    }

    static SessionKey from(ScyllaConnectorConfig config) {
      return new SessionKey(
          config.getContactPoints().toString(),
          config.getDefaultPort(),
          config.getUser(),
          config.getPassword(),
          config.getConsistencyLevel().name(),
          config.getLocalDCName(),
          config.getSslEnabled(),
          config.getSslEnabled() ? config.getSslProvider().toString() : null,
          config.getSslEnabled() ? config.getTrustStorePath() : null,
          config.getSslEnabled() ? config.getTrustStorePassword() : null,
          config.getSslEnabled() ? config.getKeyStorePath() : null,
          config.getSslEnabled() ? config.getKeyStorePassword() : null,
          config.getSslEnabled() && config.getCipherSuite() != null
              ? config.getCipherSuite().toString()
              : null,
          config.getSslEnabled() ? config.getCertPath() : null,
          config.getSslEnabled() ? config.getPrivateKeyPath() : null,
          config.getQueryOptionsFetchSize(),
          config.getPoolingCorePoolLocal(),
          config.getPoolingMaxPoolLocal(),
          config.getPoolingMaxRequestsPerConnection(),
          config.getPoolingMaxQueueSize(),
          config.getPoolingPoolTimeoutMs());
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
          && corePoolLocal == that.corePoolLocal
          && maxPoolLocal == that.maxPoolLocal
          && maxRequestsPerConnection == that.maxRequestsPerConnection
          && maxQueueSize == that.maxQueueSize
          && poolTimeoutMs == that.poolTimeoutMs
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
          fetchSize,
          corePoolLocal,
          maxPoolLocal,
          maxRequestsPerConnection,
          maxQueueSize,
          poolTimeoutMs);
    }
  }
}
