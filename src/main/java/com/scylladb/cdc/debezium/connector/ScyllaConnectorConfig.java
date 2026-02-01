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
  /** Configuration key for CDC output format. */
  public static final String CDC_OUTPUT_FORMAT_KEY = "cdc.output.format";

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

  public static final CollectionsMode DEFAULT_COLLECTIONS_MODE = CollectionsMode.DELTA;
  public static final Field COLLECTIONS_MODE = Field.create("scylla.collections.mode")
            .withDisplayName("Collections format")
            .withEnum(CollectionsMode.class, DEFAULT_COLLECTIONS_MODE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The consistency level of CDC table read queries. This consistency level is used only for read queries " +
                    "to the CDC log table.");

    public static final Field LOCAL_DC_NAME = Field.create("scylla.local.dc")
            .withDisplayName("Local DC Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The name of Scylla local datacenter. This local datacenter name will be used to setup " +
                    "the connection to Scylla to prioritize sending requests to " +
                    "the nodes in the local datacenter. If not set, no particular datacenter will be prioritized.");

    public static final CollectionsMode DEFAULT_COLLECTIONS_MODE = CollectionsMode.DELTA;
    public static final Field COLLECTIONS_MODE = Field.create("scylla.collections.mode")
            .withDisplayName("Collections format")
            .withEnum(CollectionsMode.class, DEFAULT_COLLECTIONS_MODE)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to represent non-frozen collections. Currently, only 'delta' mode is supported - in the future " +
                    "support for more modes may be added. 'Delta' mode: change in collection is represented as a struct " +
                    "with 2 fields, 'mode' and 'elements'. 'mode' describes what type of change happened (modifying collection, overwriting collection), " +
                    "'elements' contains added/removed elements.");
    /*
     * Scylla CDC Source Connector relies on heartbeats to move the offset,
     * because the offset determines if the generation ended, therefore HEARTBEAT_INTERVAL
     * should be positive (0 would disable heartbeats) and a default value is changed
     * (previously 0).
     */
    protected static final Field CUSTOM_HEARTBEAT_INTERVAL = Heartbeat.HEARTBEAT_INTERVAL
            .withDescription("Length of an interval in milli-seconds in in which the connector periodically sends heartbeat messages "
                    + "to a heartbeat topic. In Scylla CDC Source Connector, a heartbeat message is used to record the last read " +
                    "CDC log row.")
            .withDefault(30000)
            .withValidation(Field::isRequired, Field::isPositiveInteger);

    private static final ConfigDefinition CONFIG_DEFINITION =
            CommonConnectorConfig.CONFIG_DEFINITION.edit()
                    .name("Scylla")
                    .type(CLUSTER_IP_ADDRESSES, USER, PASSWORD, LOGICAL_NAME, CONSISTENCY_LEVEL, LOCAL_DC_NAME, COLLECTIONS_MODE)
                    .connector(QUERY_TIME_WINDOW_SIZE, CONFIDENCE_WINDOW_SIZE)
                    .events(TABLE_NAMES)
                    .excluding(Heartbeat.HEARTBEAT_INTERVAL).events(CUSTOM_HEARTBEAT_INTERVAL)
                    // Exclude some Debezium options, which are not applicable/not supported by
                    // the Scylla CDC Source Connector.
                    .excluding(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA,
                            CommonConnectorConfig.SNAPSHOT_DELAY_MS,
                            CommonConnectorConfig.SNAPSHOT_MODE_TABLES,
                            CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                            CommonConnectorConfig.SNAPSHOT_MAX_THREADS,
                            CommonConnectorConfig.QUERY_FETCH_SIZE)
                    .create();

  public CdcOutputFormat getCdcOutputFormat() {
    return cdcOutputFormat;
  }

  public boolean getPreimagesEnabled() {
    return config.getBoolean(ScyllaConnectorConfig.PREIMAGES_ENABLED);
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

  public CollectionsMode getCollectionsMode() {
    String value = config.getString(ScyllaConnectorConfig.COLLECTIONS_MODE);
    if (value == null) {
      return DEFAULT_COLLECTIONS_MODE;
    }
    try {
      return CollectionsMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return DEFAULT_COLLECTIONS_MODE;
    }
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

  /**
   * Enum representing the output format for CDC messages.
   *
   * <ul>
   *   <li>{@code LEGACY} - v1 format with Cell wrapper for non-PK columns ({@code {"value":
   *       <actual_value>}}), simple preimage handling via {@code experimental.preimages.enabled}
   *   <li>{@code ADVANCED} - v2 format with direct values (no Cell wrapper), supports {@code
   *       cdc.include.before/after} modes with both preimage and postimage
   * </ul>
   */
  public enum CdcOutputFormat implements EnumeratedValue {
    LEGACY("legacy"),
    ADVANCED("advanced");

    private final String value;

    CdcOutputFormat(String value) {
      this.value = value;
    }

    @Override
    public String getValue() {
      return value;
    }

    public static CdcOutputFormat parse(String value) {
      if (value == null) {
        return LEGACY;
      }
      String normalized = value.trim().toLowerCase(Locale.ROOT);
      for (CdcOutputFormat format : values()) {
        if (format.getValue().equals(normalized)) {
          return format;
        }
      }
      return LEGACY;
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

    public CollectionsMode getCollectionsMode() {
        String collectionsModeValue = config.getString(ScyllaConnectorConfig.COLLECTIONS_MODE);
        try {
            return CollectionsMode.valueOf(collectionsModeValue.toUpperCase());
        } catch (IllegalArgumentException ex) {
            return DEFAULT_COLLECTIONS_MODE;
        }
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
