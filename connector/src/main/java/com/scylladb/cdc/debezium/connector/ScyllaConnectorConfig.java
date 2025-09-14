package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.CQLConfiguration;
import com.scylladb.cdc.model.ExponentialRetryBackoffWithJitter;
import com.scylladb.cdc.model.TableName;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.EnumUtils;
import org.apache.kafka.common.config.ConfigDef;

public class ScyllaConnectorConfig extends CommonConnectorConfig {

  public static final Field WORKER_CONFIG =
      Field.create("scylla.worker.config")
          .withDescription("Internal use only")
          .withType(ConfigDef.Type.STRING)
          .withInvisibleRecommender();

  public static final Field CLUSTER_IP_ADDRESSES =
      Field.create("scylla.cluster.ip.addresses")
          .withDisplayName("Hosts")
          .withType(ConfigDef.Type.LIST)
          .withWidth(ConfigDef.Width.LONG)
          .withImportance(ConfigDef.Importance.HIGH)
          .withValidation(ConfigSerializerUtil::validateClusterIpAddresses)
          .withDescription(
              "List of IP addresses of nodes in the Scylla cluster that the connector "
                  + "will use to open initial connections to the cluster. "
                  + "In the form of a comma-separated list of pairs <IP>:<PORT>");

  public static final Field SSL_ENABLED =
      Field.create("scylla.ssl.enabled")
          .withDisplayName("SSL")
          .withType(ConfigDef.Type.BOOLEAN)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription("Flag to determine if SSL is enabled when connecting to ScyllaDB.");

  public static final Field SSL_PROVIDER =
      Field.create("scylla.ssl.provider")
          .withDisplayName("SSL Provider")
          .withEnum(SslProvider.class, SslProvider.JDK)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The SSL Provider to use when connecting to ScyllaDB. "
                  + "Valid Values are JDK, OPENSSL, OPENSSL_REFCNT.");

  public static final Field SSL_TRUSTSTORE_PATH =
      Field.create("scylla.ssl.truststore.path")
          .withDisplayName("SSL Truststore Path")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription("Path to the Java Truststore.");

  public static final Field SSL_TRUSTSTORE_PASSWORD =
      Field.create("scylla.ssl.truststore.password")
          .withDisplayName("SSL Truststore Password")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription("Password to open the Java Truststore with.");

  public static final Field SSL_KEYSTORE_PATH =
      Field.create("scylla.ssl.keystore.path")
          .withDisplayName("SSL Keystore Path")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription("Path to the Java Keystore.");

  public static final Field SSL_KEYSTORE_PASSWORD =
      Field.create("scylla.ssl.keystore.password")
          .withDisplayName("SSL Keystore Password")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription("Password to open the Java Keystore with.");

  public static final Field SSL_CIPHER_SUITES =
      Field.create("scylla.ssl.cipherSuites")
          .withDisplayName("The cipher suites to enable")
          .withType(ConfigDef.Type.LIST)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription(
              "The cipher suites to enable. "
                  + "Defaults to none, resulting in a ``minimal quality of service`` according to JDK documentation.");

  public static final Field SSL_OPENSLL_KEYCERTCHAIN =
      Field.create("scylla.ssl.openssl.keyCertChain")
          .withDisplayName("The path to the certificate chain file")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription("Path to the SSL certificate file, when using OpenSSL.");

  public static final Field SSL_OPENSLL_PRIVATEKEY =
      Field.create("scylla.ssl.openssl.privateKey")
          .withDisplayName("The path to the private key file")
          .withType(ConfigDef.Type.STRING)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription("Path to the private key file, when using OpenSSL.");

  public static final Field TABLE_NAMES =
      Field.create("scylla.table.names")
          .withDisplayName("Table Names")
          .withType(ConfigDef.Type.LIST)
          .withWidth(ConfigDef.Width.LONG)
          .withImportance(ConfigDef.Importance.HIGH)
          .withValidation(ConfigSerializerUtil::validateTableNames)
          .withDescription(
              "List of CDC-enabled table names for connector to read. "
                  + "Provided as a comma-separated list of pairs <keyspace name>.<table name>");

  public static final Field USER =
      Field.create("scylla.user")
          .withDisplayName("User")
          .withType(ConfigDef.Type.STRING)
          .withWidth(ConfigDef.Width.SHORT)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription(
              "The username to connect to Scylla with. If not set, no authorization is done.");

  public static final Field PASSWORD =
      Field.create("scylla.password")
          .withDisplayName("Password")
          .withType(ConfigDef.Type.PASSWORD)
          .withWidth(ConfigDef.Width.SHORT)
          .withImportance(ConfigDef.Importance.HIGH)
          .withDescription(
              "The password to connect to Scylla with. If not set, no authorization is done.");

  public static final Field QUERY_TIME_WINDOW_SIZE =
      Field.create("scylla.query.time.window.size")
          .withDisplayName("Query Time Window Size (ms)")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The size of windows queried by the connector. Changes are queried using SELECT statements "
                  + "with time restriction with width defined by this parameter. Value expressed in milliseconds.")
          .withValidation(Field::isNonNegativeInteger)
          .withDefault(30000);

  public static final Field CONFIDENCE_WINDOW_SIZE =
      Field.create("scylla.confidence.window.size")
          .withDisplayName("Confidence Window Size (ms)")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The size of the confidence window. It is necessary for the connector to avoid reading too fresh "
                  + "data from the CDC log due to the eventual consistency of Scylla. The problem could appear when a newer write "
                  + "reaches a replica before some older write. For a short period of time, when reading, it "
                  + "is possible for the replica to return only the newer write. The connector mitigates this problem "
                  + "by not reading a window of most recent changes (controlled by this parameter). Value expressed in milliseconds.")
          .withValidation(Field::isNonNegativeInteger)
          .withDefault(30000);

  public static final Field MINIMAL_WAIT_FOR_WINDOW_MS =
      Field.create("scylla.minimal.wait.for.window.time")
          .withDisplayName("Minimal 'waitForWindow' time (ms)")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Minimal time between reading consecutive CDC log windows. Meant to be used as a simple throttling mechanism "
                  + "in situations where driver has a lot of old data to catch up on and ends up hogging resources. "
                  + "Value expressed in milliseconds.")
          .withValidation(Field::isNonNegativeInteger)
          .withDefault(0);

  public static final CQLConfiguration.ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL =
      CQLConfiguration.ConsistencyLevel.QUORUM;
  public static final Field CONSISTENCY_LEVEL =
      Field.create("scylla.consistency.level")
          .withDisplayName("Consistency Level")
          .withEnum(CQLConfiguration.ConsistencyLevel.class, DEFAULT_CONSISTENCY_LEVEL)
          .withWidth(ConfigDef.Width.SHORT)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription(
              "The consistency level of CDC table read queries. This consistency level is used only for read queries "
                  + "to the CDC log table.");
  public static final Field QUERY_OPTIONS_FETCH_SIZE =
      Field.create("scylla.query.options.fetch.size")
          .withDisplayName("Queries fetch size")
          .withType(ConfigDef.Type.INT)
          .withDefault(0)
          .withWidth(ConfigDef.Width.SHORT)
          .withImportance(ConfigDef.Importance.LOW)
          .withValidation(Field::isNonNegativeInteger)
          .withDescription(
              "The default page fetch size for all driver select queries. Value 0 means use driver "
                  + "defaults (usually 5000). Passed to "
                  + "driver's QueryOptions before session construction. Set this to an explicit value if "
                  + "experiencing too high memory usage.");

  public static final Field LOCAL_DC_NAME =
      Field.create("scylla.local.dc")
          .withDisplayName("Local DC Name")
          .withType(ConfigDef.Type.STRING)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The name of Scylla local datacenter. This local datacenter name will be used to setup "
                  + "the connection to Scylla to prioritize sending requests to "
                  + "the nodes in the local datacenter. If not set, no particular datacenter will be prioritized.");

  public static final Field PREIMAGES_ENABLED =
      Field.create("experimental.preimages.enabled")
          .withDisplayName("Enable experimental preimages support")
          .withType(ConfigDef.Type.BOOLEAN)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault(false)
          .withDescription(
              "If enabled connector will use PRE_IMAGE CDC entries to populate 'before' field of the "
                  + "debezium Envelope of the next kafka message. This may change some expected behaviours (e.g. ROW_DELETE "
                  + "will use preimage instead of its own information). See Scylla docs for more information about CDC "
                  + "preimages limitations. ");

  /*
   * Scylla CDC Source Connector relies on heartbeats to move the offset,
   * because the offset determines if the generation ended, therefore HEARTBEAT_INTERVAL
   * should be positive (0 would disable heartbeats) and a default value is changed
   * (previously 0).
   */
  protected static final Field CUSTOM_HEARTBEAT_INTERVAL =
      Heartbeat.HEARTBEAT_INTERVAL
          .withDescription(
              "Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                  + "to a heartbeat topic. In Scylla CDC Source Connector, a heartbeat message is used to record the last read "
                  + "CDC log row.")
          .withDefault(30000)
          .withValidation(Field::isRequired, Field::isPositiveInteger);

  public static final Field SOURCE_INFO_STRUCT_MAKER =
      CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER.withDefault(
          ScyllaSourceInfoStructMaker.class.getName());

  public static final Field RETRY_BACKOFF_BASE_MS =
      Field.create("worker.retry.backoff.base")
          .withDisplayName("Worker's retry base backoff (ms)")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The initial backoff in milliseconds that will be used for queries to Scylla. "
                  + "Each consecutive retry will increase exponentially by a factor of 2 up to configured max backoff.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(50);

  public static final Field RETRY_MAX_BACKOFF_MS =
      Field.create("worker.maximum.backoff")
          .withDisplayName("Worker's retry maximum backoff (ms)")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Maximum backoff in milliseconds that will be used for queries to Scylla.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(30000);

  public static final Field RETRY_BACKOFF_JITTER_PERCENTAGE =
      Field.create("worker.jitter.percentage")
          .withDisplayName("Worker's retry jitter percentage")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "The jitter applied to the retry backoffs to make them more spread out in case of the "
                  + "surges in retries performed. Setting 20 (20%) means that the backoff will have randomly up to 20% of its value "
                  + "subtracted before application. The jitter does not modify base backoff and has no impact on exponential rise. "
                  + "Minimal allowed value is 1. Max is 100.")
          .withValidation(Field::isPositiveInteger)
          .optional()
          .withDefault(20);

  public static final Field POOLING_CORE_POOL_LOCAL =
      Field.create("worker.pooling.core.pool.local")
          .withDisplayName("Number of connections to DB node")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Worker's target number of connections to a singular Scylla node within distance 'LOCAL'. "
                  + "Local nodes are the nodes of local datacenter. "
                  + "Driver session used by worker will aim to maintain this number of connections per local node.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(1);

  public static final Field POOLING_MAX_POOL_LOCAL =
      Field.create("worker.pooling.max.pool.local")
          .withDisplayName("Max number of connections to DB node")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Worker's maximum number of connections to a singular Scylla node within distance 'LOCAL'. "
                  + "Worker will open additional connections up to this maximum whenever existing ones go above certain threshold"
                  + " of concurrent requests.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(1);

  public static final Field POOLING_MAX_QUEUE_SIZE =
      Field.create("worker.pooling.max.queue.size")
          .withDisplayName("Max requests queue size")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Worker's maximum requests queue size per connection pool. Requests get enqueued if no connection "
                  + "is available. For some setups (many nodes, many shards, few connector tasks, few connections) it may "
                  + "be necessary to increase this to avoid BusyPoolException. Additional requests above this limit will be "
                  + "rejected. Requests that wait for longer than pool timeout value also will be rejected.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(512);

  public static final Field POOLING_MAX_REQUESTS_PER_CONNECTION =
      Field.create("worker.pooling.max.requests.per.connection")
          .withDisplayName("Max requests per DB connection")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Worker's maximum requests per connection to a Scylla node within distance 'LOCAL'. Requests above "
                  + "this quantity will be enqueued.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(1024);

  public static final Field POOLING_POOL_TIMEOUT_MS =
      Field.create("worker.pooling.pool.timeout.ms")
          .withDisplayName("Timeout for acquiring a DB connection")
          .withType(ConfigDef.Type.INT)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Worker's timeout for trying to acquire a connection from a host's pool.")
          .withValidation(Field::isNonNegativeInteger)
          .optional()
          .withDefault(5000);

  private static final ConfigDefinition CONFIG_DEFINITION =
      CommonConnectorConfig.CONFIG_DEFINITION
          .edit()
          .name("Scylla")
          .type(
              CLUSTER_IP_ADDRESSES,
              USER,
              PASSWORD,
              TOPIC_PREFIX,
              CONSISTENCY_LEVEL,
              QUERY_OPTIONS_FETCH_SIZE,
              LOCAL_DC_NAME,
              SSL_ENABLED,
              SSL_PROVIDER,
              SSL_TRUSTSTORE_PATH,
              SSL_TRUSTSTORE_PASSWORD,
              SSL_KEYSTORE_PATH,
              SSL_KEYSTORE_PASSWORD,
              SSL_CIPHER_SUITES,
              SSL_OPENSLL_KEYCERTCHAIN,
              SSL_OPENSLL_PRIVATEKEY)
          .connector(
              QUERY_TIME_WINDOW_SIZE,
              CONFIDENCE_WINDOW_SIZE,
              MINIMAL_WAIT_FOR_WINDOW_MS,
              PREIMAGES_ENABLED,
              RETRY_BACKOFF_BASE_MS,
              RETRY_MAX_BACKOFF_MS,
              RETRY_BACKOFF_JITTER_PERCENTAGE,
              POOLING_CORE_POOL_LOCAL,
              POOLING_MAX_POOL_LOCAL,
              POOLING_MAX_REQUESTS_PER_CONNECTION,
              POOLING_MAX_QUEUE_SIZE,
              POOLING_POOL_TIMEOUT_MS)
          .events(TABLE_NAMES)
          .excluding(Heartbeat.HEARTBEAT_INTERVAL)
          .events(CUSTOM_HEARTBEAT_INTERVAL)
          // Exclude some Debezium options, which are not applicable/not supported by
          // the Scylla CDC Source Connector.
          .excluding(
              CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
              CommonConnectorConfig.SNAPSHOT_DELAY_MS,
              CommonConnectorConfig.SNAPSHOT_MODE_TABLES,
              CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
              CommonConnectorConfig.SNAPSHOT_MAX_THREADS,
              CommonConnectorConfig.QUERY_FETCH_SIZE)
          .create();

  protected static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

  protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

  private final Configuration config;

  protected ScyllaConnectorConfig(Configuration config) {
    super(config, 0);
    this.config = config;
  }

  public static ConfigDef configDef() {
    return CONFIG_DEFINITION.configDef();
  }

  public List<InetSocketAddress> getContactPoints() {
    return ConfigSerializerUtil.deserializeClusterIpAddresses(
        config.getString(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES));
  }

  public boolean getSslEnabled() {
    return config.getBoolean(SSL_ENABLED);
  }

  public SslProvider getSslProvider() {
    return EnumUtils.getEnum(SslProvider.class, config.getString(SSL_PROVIDER).toUpperCase());
  }

  public String getTrustStorePath() {
    return config.getString(SSL_TRUSTSTORE_PATH);
  }

  public String getTrustStorePassword() {
    return config.getString(SSL_TRUSTSTORE_PASSWORD);
  }

  public String getKeyStorePath() {
    return config.getString(SSL_KEYSTORE_PATH);
  }

  public String getKeyStorePassword() {
    return config.getString(SSL_KEYSTORE_PASSWORD);
  }

  public List<String> getCipherSuite() {
    return config.getInstance(SSL_CIPHER_SUITES, List.class);
  }

  public String getCertPath() {
    return config.getString(SSL_OPENSLL_KEYCERTCHAIN);
  }

  public String getPrivateKeyPath() {
    return config.getString(SSL_OPENSLL_PRIVATEKEY);
  }

  public Set<TableName> getTableNames() {
    return ConfigSerializerUtil.deserializeTableNames(
        config.getString(ScyllaConnectorConfig.TABLE_NAMES));
  }

  public String getUser() {
    return config.getString(ScyllaConnectorConfig.USER);
  }

  public String getPassword() {
    return config.getString(ScyllaConnectorConfig.PASSWORD);
  }

  public long getQueryTimeWindowSizeMs() {
    return config.getInteger(ScyllaConnectorConfig.QUERY_TIME_WINDOW_SIZE);
  }

  public long getConfidenceWindowSizeMs() {
    return config.getInteger(ScyllaConnectorConfig.CONFIDENCE_WINDOW_SIZE);
  }

  public long getMinimalWaitForWindowMs() {
    return config.getInteger(ScyllaConnectorConfig.MINIMAL_WAIT_FOR_WINDOW_MS);
  }

  public long getHeartbeatIntervalMs() {
    return config.getInteger(Heartbeat.HEARTBEAT_INTERVAL);
  }

  public CQLConfiguration.ConsistencyLevel getConsistencyLevel() {
    String consistencyLevelValue = config.getString(ScyllaConnectorConfig.CONSISTENCY_LEVEL);
    try {
      return CQLConfiguration.ConsistencyLevel.valueOf(consistencyLevelValue.toUpperCase());
    } catch (IllegalArgumentException ex) {
      return DEFAULT_CONSISTENCY_LEVEL;
    }
  }

  public String getLocalDCName() {
    return config.getString(ScyllaConnectorConfig.LOCAL_DC_NAME);
  }

  public boolean getPreimagesEnabled() {
    return config.getBoolean(ScyllaConnectorConfig.PREIMAGES_ENABLED);
  }

  public int getQueryOptionsFetchSize() {
    return config.getInteger(ScyllaConnectorConfig.QUERY_OPTIONS_FETCH_SIZE);
  }

  public int getRetryBackoffBaseMs() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_BACKOFF_BASE_MS);
  }

  public int getRetryMaxBackoffMs() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_MAX_BACKOFF_MS);
  }

  public int getRetryBackoffJitterPercentage() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_BACKOFF_JITTER_PERCENTAGE);
  }

  public ExponentialRetryBackoffWithJitter createCDCWorkerRetryBackoff() {
    int backoffMs = getRetryBackoffBaseMs();
    int maxBackoffMs = getRetryMaxBackoffMs();
    double jitter = getRetryBackoffJitterPercentage() / 100.0;
    return new ExponentialRetryBackoffWithJitter(backoffMs, maxBackoffMs, jitter);
  }

  public int getPoolingCorePoolLocal() {
    return config.getInteger(POOLING_CORE_POOL_LOCAL);
  }

  public int getPoolingMaxPoolLocal() {
    return config.getInteger(POOLING_MAX_POOL_LOCAL);
  }

  public int getPoolingMaxRequestsPerConnection() {
    return config.getInteger(POOLING_MAX_REQUESTS_PER_CONNECTION);
  }

  public int getPoolingMaxQueueSize() {
    return config.getInteger(POOLING_MAX_QUEUE_SIZE);
  }

  public int getPoolingPoolTimeoutMs() {
    return config.getInteger(POOLING_POOL_TIMEOUT_MS);
  }

  public int getDefaultPort() {
    for (InetSocketAddress cp : this.getContactPoints()) {
      if (cp.getPort() != 0) {
        return cp.getPort();
      }
    }
    return 9042;
  }

  @Override
  public String getContextName() {
    return "Scylla";
  }

  @Override
  public String getConnectorName() {
    return "scylla";
  }

  public enum SnapshotMode implements EnumeratedValue {
    INITIAL("initial");

    private final String value;

    SnapshotMode(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }
  }

  @Override
  public EnumeratedValue getSnapshotMode() {
    return SnapshotMode.INITIAL;
  }

  @Override
  public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
    return Optional.empty();
  }

  @Override
  protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
    return getSourceInfoStructMaker(
        SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
  }
}
