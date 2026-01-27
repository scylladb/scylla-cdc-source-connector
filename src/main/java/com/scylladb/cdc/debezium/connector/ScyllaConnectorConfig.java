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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.EnumUtils;
import org.apache.kafka.common.config.ConfigDef;

public class ScyllaConnectorConfig extends CommonConnectorConfig {

  // Configuration key constants
  /** Configuration key for CDC include before mode. */
  public static final String CDC_INCLUDE_BEFORE_KEY = "cdc.include.before";

  /** Configuration key for CDC include after mode. */
  public static final String CDC_INCLUDE_AFTER_KEY = "cdc.include.after";

  /** Configuration key for CDC include PK locations. */
  public static final String CDC_INCLUDE_PK_KEY = "cdc.include.primary-key.placement";

  /** Configuration key for CDC include PK payload key field name. */
  public static final String CDC_INCLUDE_PK_PAYLOAD_KEY_NAME_KEY =
      "cdc.include.primary-key.payload-key-name";

  /** Default value for the payload key field name. */
  public static final String CDC_INCLUDE_PK_PAYLOAD_KEY_NAME_DEFAULT = "key";

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

  public static final Field CDC_INCLUDE_BEFORE =
      Field.create(CDC_INCLUDE_BEFORE_KEY)
          .withDisplayName("CDC Include Before")
          .withEnum(CdcIncludeMode.class, CdcIncludeMode.NONE)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription(
              "Specifies whether to include the 'before' state of the row in CDC messages. "
                  + "Set to 'full' to include the complete row before the change. "
                  + "Requires the table to have preimage enabled (WITH cdc = {'preimage': true}). "
                  + "Default is 'none'.");

  public static final Field CDC_INCLUDE_AFTER =
      Field.create(CDC_INCLUDE_AFTER_KEY)
          .withDisplayName("CDC Include After")
          .withEnum(CdcIncludeMode.class, CdcIncludeMode.NONE)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDescription(
              "Specifies whether to include the 'after' state of the row in CDC messages. "
                  + "Set to 'full' to include the complete row after the change. "
                  + "Requires the table to have postimage enabled (WITH cdc = {'postimage': true}). "
                  + "Default is 'none'.");

  public static final Field CDC_INCLUDE_PK =
      Field.create(CDC_INCLUDE_PK_KEY)
          .withDisplayName("CDC Include PK Locations")
          .withType(ConfigDef.Type.LIST)
          .withWidth(ConfigDef.Width.LONG)
          .withImportance(ConfigDef.Importance.MEDIUM)
          .withDefault("kafka-key,payload-after,payload-before")
          .withValidation(Field::isRequired, ConfigSerializerUtil::validateCdcIncludePk)
          .withDescription(
              "Specifies where primary key (PK) and clustering key (CK) columns should be included "
                  + "in the output. Provide as a comma-separated list of locations: "
                  + "'kafka-key' (Kafka record key for partitioning/ordering), "
                  + "'payload-after' (inside value.after), "
                  + "'payload-before' (inside value.before), "
                  + "'payload-key' (top-level key object in message value), "
                  + "'kafka-headers' (as Kafka message headers). "
                  + "Default is 'kafka-key,payload-after,payload-before'.");

  public static final Field CDC_INCLUDE_PK_PAYLOAD_KEY_NAME =
      Field.create(CDC_INCLUDE_PK_PAYLOAD_KEY_NAME_KEY)
          .withDisplayName("CDC Include PK Payload Key Field Name")
          .withType(ConfigDef.Type.STRING)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDefault(CDC_INCLUDE_PK_PAYLOAD_KEY_NAME_DEFAULT)
          .withDescription(
              "Specifies the field name for the primary key object in the message payload "
                  + "when 'payload-key' is included in cdc.include.primary-key.placement. "
                  + "Default is 'key'.");

  public static final Field CDC_INCOMPLETE_TASK_TIMEOUT_MS =
      Field.create("cdc.incomplete.task.timeout.ms")
          .withDisplayName("Incomplete Task Timeout (ms)")
          .withType(ConfigDef.Type.LONG)
          .withWidth(ConfigDef.Width.MEDIUM)
          .withImportance(ConfigDef.Importance.LOW)
          .withDescription(
              "Timeout in milliseconds for incomplete CDC tasks waiting for preimage/postimage events. "
                  + "Tasks that remain incomplete longer than this duration will be dropped and logged as errors. "
                  + "Default is 15000 (15 seconds).")
          .withValidation(Field::isPositiveLong)
          .withDefault(15000L);

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
              CDC_INCLUDE_BEFORE,
              CDC_INCLUDE_AFTER,
              CDC_INCLUDE_PK,
              CDC_INCLUDE_PK_PAYLOAD_KEY_NAME,
              CDC_INCOMPLETE_TASK_TIMEOUT_MS,
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
  private final CdcIncludeMode cdcIncludeBefore;
  private final CdcIncludeMode cdcIncludeAfter;
  private final PkLocationConfig pkLocationConfig;

  /** Pre-computed boolean flags for PK location configuration to avoid repeated EnumSet lookups. */
  public static class PkLocationConfig {
    public final boolean inKafkaKey;
    public final boolean inPayloadAfter;
    public final boolean inPayloadBefore;
    public final boolean inPayloadKey;
    public final boolean inKafkaHeaders;

    PkLocationConfig(EnumSet<CdcIncludePkLocation> locations) {
      this.inKafkaKey = locations.contains(CdcIncludePkLocation.KAFKA_KEY);
      this.inPayloadAfter = locations.contains(CdcIncludePkLocation.PAYLOAD_AFTER);
      this.inPayloadBefore = locations.contains(CdcIncludePkLocation.PAYLOAD_BEFORE);
      this.inPayloadKey = locations.contains(CdcIncludePkLocation.PAYLOAD_KEY);
      this.inKafkaHeaders = locations.contains(CdcIncludePkLocation.KAFKA_HEADERS);
    }
  }

  /** Builds a connector configuration wrapper around the Debezium config. */
  protected ScyllaConnectorConfig(Configuration config) {
    super(config, 0);
    this.config = config;
    this.cdcIncludeBefore =
        CdcIncludeMode.parse(config.getString(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE));
    this.cdcIncludeAfter =
        CdcIncludeMode.parse(config.getString(ScyllaConnectorConfig.CDC_INCLUDE_AFTER));
    this.pkLocationConfig =
        new PkLocationConfig(
            CdcIncludePkLocation.parseList(config.getList(ScyllaConnectorConfig.CDC_INCLUDE_PK)));
  }

  /** Returns the connector configuration definition for Kafka Connect. */
  public static ConfigDef configDef() {
    return CONFIG_DEFINITION.configDef();
  }

  /** Returns the configured Scylla contact points (host:port). */
  public List<InetSocketAddress> getContactPoints() {
    return ConfigSerializerUtil.deserializeClusterIpAddresses(
        config.getString(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES));
  }

  /** Returns whether SSL is enabled for Scylla connections. */
  public boolean getSslEnabled() {
    return config.getBoolean(SSL_ENABLED);
  }

  /** Returns the SSL provider to use for Scylla connections. */
  public SslProvider getSslProvider() {
    return EnumUtils.getEnum(SslProvider.class, config.getString(SSL_PROVIDER).toUpperCase());
  }

  /** Returns the configured truststore path, if any. */
  public String getTrustStorePath() {
    return config.getString(SSL_TRUSTSTORE_PATH);
  }

  /** Returns the configured truststore password, if any. */
  public String getTrustStorePassword() {
    return config.getString(SSL_TRUSTSTORE_PASSWORD);
  }

  /** Returns the configured keystore path, if any. */
  public String getKeyStorePath() {
    return config.getString(SSL_KEYSTORE_PATH);
  }

  /** Returns the configured keystore password, if any. */
  public String getKeyStorePassword() {
    return config.getString(SSL_KEYSTORE_PASSWORD);
  }

  /** Returns the configured SSL cipher suites list. */
  public List<String> getCipherSuite() {
    return config.getList(SSL_CIPHER_SUITES);
  }

  /** Returns the OpenSSL certificate chain path, if any. */
  public String getCertPath() {
    return config.getString(SSL_OPENSLL_KEYCERTCHAIN);
  }

  /** Returns the OpenSSL private key path, if any. */
  public String getPrivateKeyPath() {
    return config.getString(SSL_OPENSLL_PRIVATEKEY);
  }

  /** Returns the configured CDC-enabled table names. */
  public Set<TableName> getTableNames() {
    return ConfigSerializerUtil.deserializeTableNames(
        config.getString(ScyllaConnectorConfig.TABLE_NAMES));
  }

  /** Returns the username for Scylla authentication, if set. */
  public String getUser() {
    return config.getString(ScyllaConnectorConfig.USER);
  }

  /** Returns the password for Scylla authentication, if set. */
  public String getPassword() {
    return config.getString(ScyllaConnectorConfig.PASSWORD);
  }

  /** Returns the CDC query time window size in milliseconds. */
  public long getQueryTimeWindowSizeMs() {
    return config.getInteger(ScyllaConnectorConfig.QUERY_TIME_WINDOW_SIZE);
  }

  /** Returns the CDC confidence window size in milliseconds. */
  public long getConfidenceWindowSizeMs() {
    return config.getInteger(ScyllaConnectorConfig.CONFIDENCE_WINDOW_SIZE);
  }

  /** Returns the minimal wait time between CDC windows in milliseconds. */
  public long getMinimalWaitForWindowMs() {
    return config.getInteger(ScyllaConnectorConfig.MINIMAL_WAIT_FOR_WINDOW_MS);
  }

  /** Returns the heartbeat interval in milliseconds. */
  public long getHeartbeatIntervalMs() {
    return config.getInteger(Heartbeat.HEARTBEAT_INTERVAL);
  }

  /** Returns the configured consistency level, falling back to the default when invalid. */
  public CQLConfiguration.ConsistencyLevel getConsistencyLevel() {
    String consistencyLevelValue = config.getString(ScyllaConnectorConfig.CONSISTENCY_LEVEL);
    try {
      return CQLConfiguration.ConsistencyLevel.valueOf(consistencyLevelValue.toUpperCase());
    } catch (IllegalArgumentException ex) {
      return DEFAULT_CONSISTENCY_LEVEL;
    }
  }

  /** Returns the local datacenter name, if configured. */
  public String getLocalDCName() {
    return config.getString(ScyllaConnectorConfig.LOCAL_DC_NAME);
  }

  public CdcIncludeMode getCdcIncludeBefore() {
    return cdcIncludeBefore;
  }

  public CdcIncludeMode getCdcIncludeAfter() {
    return cdcIncludeAfter;
  }

  public PkLocationConfig getCdcIncludePk() {
    return pkLocationConfig;
  }

  public String getCdcIncludePkPayloadKeyName() {
    return config.getString(ScyllaConnectorConfig.CDC_INCLUDE_PK_PAYLOAD_KEY_NAME);
  }

  public long getIncompleteTaskTimeoutMs() {
    return config.getLong(ScyllaConnectorConfig.CDC_INCOMPLETE_TASK_TIMEOUT_MS);
  }

  /** Returns the driver query fetch size for CDC reads. */
  public int getQueryOptionsFetchSize() {
    return config.getInteger(ScyllaConnectorConfig.QUERY_OPTIONS_FETCH_SIZE);
  }

  /** Returns the base retry backoff in milliseconds. */
  public int getRetryBackoffBaseMs() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_BACKOFF_BASE_MS);
  }

  /** Returns the maximum retry backoff in milliseconds. */
  public int getRetryMaxBackoffMs() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_MAX_BACKOFF_MS);
  }

  /** Returns the retry jitter percentage. */
  public int getRetryBackoffJitterPercentage() {
    return config.getInteger(ScyllaConnectorConfig.RETRY_BACKOFF_JITTER_PERCENTAGE);
  }

  /** Builds the retry backoff policy for CDC worker queries. */
  public ExponentialRetryBackoffWithJitter createCDCWorkerRetryBackoff() {
    int backoffMs = getRetryBackoffBaseMs();
    int maxBackoffMs = getRetryMaxBackoffMs();
    double jitter = getRetryBackoffJitterPercentage() / 100.0;
    return new ExponentialRetryBackoffWithJitter(backoffMs, maxBackoffMs, jitter);
  }

  /** Returns the core connection pool size for local nodes. */
  public int getPoolingCorePoolLocal() {
    return config.getInteger(POOLING_CORE_POOL_LOCAL);
  }

  /** Returns the maximum connection pool size for local nodes. */
  public int getPoolingMaxPoolLocal() {
    return config.getInteger(POOLING_MAX_POOL_LOCAL);
  }

  /** Returns the maximum requests per connection. */
  public int getPoolingMaxRequestsPerConnection() {
    return config.getInteger(POOLING_MAX_REQUESTS_PER_CONNECTION);
  }

  /** Returns the maximum queue size for pooled connections. */
  public int getPoolingMaxQueueSize() {
    return config.getInteger(POOLING_MAX_QUEUE_SIZE);
  }

  /** Returns the timeout to acquire a pooled connection in milliseconds. */
  public int getPoolingPoolTimeoutMs() {
    return config.getInteger(POOLING_POOL_TIMEOUT_MS);
  }

  /** Returns the default port to use when none is specified in contact points. */
  public int getDefaultPort() {
    for (InetSocketAddress cp : this.getContactPoints()) {
      if (cp.getPort() != 0) {
        return cp.getPort();
      }
    }
    return 9042;
  }

  /** {@inheritDoc} */
  @Override
  public String getContextName() {
    return "Scylla";
  }

  /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public String getValue() {
      return value;
    }
  }

  public enum CdcIncludeMode implements EnumeratedValue {
    NONE("none"),
    FULL("full"),
    ONLY_UPDATED("only-updated");

    private final String value;

    CdcIncludeMode(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    /**
     * Returns true if this mode requires preimage/postimage data.
     *
     * @return true for FULL and ONLY_UPDATED modes, false for NONE
     */
    public boolean requiresImage() {
      return this == FULL || this == ONLY_UPDATED;
    }

    public static CdcIncludeMode parse(String value) {
      if (value == null) {
        return NONE;
      }
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      for (CdcIncludeMode mode : values()) {
        if (mode.getValue().equals(normalized)) {
          return mode;
        }
      }
      return NONE;
    }
  }

  /**
   * Enum representing possible locations where PK/CK values can be included in the output.
   *
   * <p>Each location controls a specific placement of primary key and clustering key values:
   *
   * <ul>
   *   <li>{@code KAFKA_KEY} - Include in Kafka record key (for partitioning/ordering/compaction)
   *   <li>{@code PAYLOAD_AFTER} - Include in the 'after' image within the message value
   *   <li>{@code PAYLOAD_BEFORE} - Include in the 'before' image within the message value
   *   <li>{@code PAYLOAD_DIFF} - Reserved for future use (not yet implemented)
   *   <li>{@code PAYLOAD_KEY} - Include in a top-level 'key' object within the message value
   *   <li>{@code KAFKA_HEADERS} - Include as Kafka message headers
   * </ul>
   */
  public enum CdcIncludePkLocation implements EnumeratedValue {
    KAFKA_KEY("kafka-key"),
    PAYLOAD_AFTER("payload-after"),
    PAYLOAD_BEFORE("payload-before"),
    PAYLOAD_DIFF("payload-diff"),
    PAYLOAD_KEY("payload-key"),
    KAFKA_HEADERS("kafka-headers");

    private final String value;

    CdcIncludePkLocation(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    public static CdcIncludePkLocation parse(String value) {
      if (value == null) {
        return null;
      }
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      for (CdcIncludePkLocation location : values()) {
        if (location.getValue().equals(normalized)) {
          return location;
        }
      }
      return null;
    }

    /**
     * Parses a comma-separated list of location values into an EnumSet.
     *
     * @param values comma-separated list of location values
     * @return EnumSet containing the parsed locations, or empty set if none valid
     */
    public static EnumSet<CdcIncludePkLocation> parseList(List<String> values) {
      EnumSet<CdcIncludePkLocation> result = EnumSet.noneOf(CdcIncludePkLocation.class);
      if (values == null) {
        return result;
      }
      for (String value : values) {
        CdcIncludePkLocation location = parse(value);
        if (location != null) {
          result.add(location);
        }
      }
      return result;
    }
  }

  /** {@inheritDoc} */
  @Override
  public EnumeratedValue getSnapshotMode() {
    return SnapshotMode.INITIAL;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
    return getSourceInfoStructMaker(
        SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
  }
}
