package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3Session;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
 * <p>When multiple tasks share a session, the connection pool is dynamically scaled: each new task
 * that joins increases the maximum number of connections per host (via {@code
 * PoolingOptions.setMaxConnectionsPerHost} and {@code setCoreConnectionsPerHost}), and each task
 * that leaves decreases it. This ensures the shared pool can handle the aggregate concurrency of
 * all tasks without hitting {@code BusyPoolException}, while still bounding the total number of
 * connections proportionally to the actual number of tasks.
 *
 * <p>The session cache key is derived from connection-identity properties only (contact points,
 * port, credentials, SSL, consistency level, local DC, and fetch size). Pooling parameters are
 * intentionally excluded from the key because they are managed dynamically as tasks join and leave.
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
   * already exists in the cache, its reference count is incremented, the connection pool is scaled
   * up, and the existing session is returned. Otherwise, a new session is created and cached.
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
                scalePool(existing.session(), existing.refCount());
                LOGGER.info(
                    "Reusing shared CQL session for {} (ref count: {})",
                    k.summary(),
                    existing.refCount());
                return existing;
              }
              LOGGER.info("Creating new shared CQL session for {}", k.summary());
              Driver3Session session = new ScyllaSessionBuilder(configuration).build();
              return new RefCountedSession(
                  session,
                  configuration.getPoolingCorePoolLocal(),
                  configuration.getPoolingMaxPoolLocal());
            });
    return refCounted.session();
  }

  /**
   * Releases a previously acquired session. Decrements the reference count, scales down the
   * connection pool, and closes the session if no more tasks are using it.
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
          scalePool(existing.session(), remaining);
          LOGGER.info("Released shared CQL session for {} (ref count: {})", k.summary(), remaining);
          return existing;
        });
  }

  /**
   * Scales the connection pool to accommodate the given number of tasks sharing the session. Sets
   * {@code maxConnectionsPerHost} and {@code coreConnectionsPerHost} to {@code baseCorePool *
   * taskCount} and {@code baseMaxPool * taskCount} respectively, so each task gets its fair share
   * of connection capacity.
   *
   * <p>Uses reflection to access the underlying driver's {@code PoolingOptions} through the shaded
   * class hierarchy: {@code Driver3Session.driverSession} -> {@code Session.getCluster()} -> {@code
   * Cluster.getConfiguration()} -> {@code Configuration.getPoolingOptions()}.
   *
   * <p>The DataStax Java Driver 3.x {@code PoolingOptions} fields are {@code volatile} and designed
   * for runtime modification. {@code setCoreConnectionsPerHost} additionally triggers {@code
   * Manager.ensurePoolsSizing()} which actively opens new connections if needed.
   */
  private static void scalePool(Driver3Session driver3Session, int taskCount) {
    try {
      // Access the raw driver Session from Driver3Session via reflection.
      // Driver3Session stores the DataStax Session as a private field.
      Field driverSessionField = Driver3Session.class.getDeclaredField("driverSession");
      driverSessionField.setAccessible(true);
      Object rawSession = driverSessionField.get(driver3Session);

      // Navigate the driver object graph to reach PoolingOptions:
      // Session -> Cluster -> Configuration -> PoolingOptions
      Object cluster = rawSession.getClass().getMethod("getCluster").invoke(rawSession);
      Object configuration = cluster.getClass().getMethod("getConfiguration").invoke(cluster);
      Object poolingOptions =
          configuration.getClass().getMethod("getPoolingOptions").invoke(configuration);

      // Find the HostDistance enum class from the shaded driver package.
      // We locate it via the parameter type of getCoreConnectionsPerHost(HostDistance).
      Method getCoreMethod = findMethod(poolingOptions.getClass(), "getCoreConnectionsPerHost");
      Class<?> hostDistanceClass = getCoreMethod.getParameterTypes()[0];
      Object localDistance = hostDistanceClass.getField("LOCAL").get(null);

      // Read current pool values
      Method getMaxMethod = findMethod(poolingOptions.getClass(), "getMaxConnectionsPerHost");
      int currentCore = (int) getCoreMethod.invoke(poolingOptions, localDistance);
      int currentMax = (int) getMaxMethod.invoke(poolingOptions, localDistance);

      // Compute the per-task base values from the RefCountedSession
      // (stored at session creation time from the first task's config)
      RefCountedSession refCounted = null;
      for (RefCountedSession rc : CACHE.values()) {
        if (rc.session() == driver3Session) {
          refCounted = rc;
          break;
        }
      }
      int baseCorePool = refCounted != null ? refCounted.baseCorePool : 1;
      int baseMaxPool = refCounted != null ? refCounted.baseMaxPool : 1;

      int newCore = baseCorePool * taskCount;
      int newMax = baseMaxPool * taskCount;

      // The driver enforces core <= max, so the order of updates matters.
      // When scaling up: increase max first, then core.
      // When scaling down: decrease core first, then max.
      Method setCoreMethod =
          findMethod(poolingOptions.getClass(), "setCoreConnectionsPerHost", int.class);
      Method setMaxMethod =
          findMethod(poolingOptions.getClass(), "setMaxConnectionsPerHost", int.class);

      if (newMax >= currentMax) {
        // Scaling up or same: max first, then core
        setMaxMethod.invoke(poolingOptions, localDistance, newMax);
        setCoreMethod.invoke(poolingOptions, localDistance, newCore);
      } else {
        // Scaling down: core first, then max
        setCoreMethod.invoke(poolingOptions, localDistance, newCore);
        setMaxMethod.invoke(poolingOptions, localDistance, newMax);
      }

      LOGGER.info(
          "Scaled connection pool for {} tasks: core={}, max={}", taskCount, newCore, newMax);
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to scale connection pool for {} tasks. "
              + "The session will continue working with the existing pool size, "
              + "which may cause BusyPoolException under high concurrency.",
          taskCount,
          e);
    }
  }

  /**
   * Finds a method by name on the given class, matching by name only (for getter-style methods with
   * a single HostDistance parameter) or by name and additional parameter types (for setter-style
   * methods). This avoids hard-coding the shaded HostDistance class name.
   */
  private static Method findMethod(Class<?> clazz, String name, Class<?>... extraParamTypes) {
    for (Method m : clazz.getMethods()) {
      if (!m.getName().equals(name)) continue;
      Class<?>[] params = m.getParameterTypes();
      if (params.length != 1 + extraParamTypes.length) continue;
      // First param must be an enum (HostDistance)
      if (!params[0].isEnum()) continue;
      // Check remaining params match
      boolean match = true;
      for (int i = 0; i < extraParamTypes.length; i++) {
        if (!params[i + 1].equals(extraParamTypes[i])) {
          match = false;
          break;
        }
      }
      if (match) return m;
    }
    throw new NoSuchMethodError(
        "Method " + name + " not found on " + clazz.getName() + " with expected parameter types");
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
   * the reference count drops to zero. Also stores the per-task base pooling values so the pool can
   * be scaled proportionally as tasks join and leave.
   */
  private static final class RefCountedSession {
    private final Driver3Session session;
    private final AtomicInteger refs;
    private volatile boolean closed;

    /** Per-task base value for core connections per host (from the first task's config). */
    final int baseCorePool;

    /** Per-task base value for max connections per host (from the first task's config). */
    final int baseMaxPool;

    RefCountedSession(Driver3Session session, int baseCorePool, int baseMaxPool) {
      this.session = session;
      this.refs = new AtomicInteger(1);
      this.closed = false;
      this.baseCorePool = Math.max(1, baseCorePool);
      this.baseMaxPool = Math.max(1, baseMaxPool);
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
   * key if and only if they connect to the same cluster with the same identity (credentials, SSL,
   * consistency, fetch size). Pooling parameters are intentionally excluded because they are
   * managed dynamically by the cache as tasks join and leave.
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
