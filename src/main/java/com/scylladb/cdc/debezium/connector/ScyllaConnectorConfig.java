package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TableName;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import org.apache.kafka.common.config.ConfigDef;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

public class ScyllaConnectorConfig extends CommonConnectorConfig {

    public static final Field WORKER_CONFIG = Field.create("scylla.worker.config")
            .withDescription("Internal use only")
            .withType(ConfigDef.Type.STRING)
            .withInvisibleRecommender();

    public static final Field LOGICAL_NAME = Field.create("scylla.name")
            .withDisplayName("Namespace")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Unique name that identifies the Scylla cluster and "
                    + "that is used as a prefix for all schemas and topics. "
                    + "Each distinct Scylla installation should have a separate namespace and be monitored by "
                    + "at most one Debezium connector.");

    public static final Field CLUSTER_IP_ADDRESSES = Field.create("scylla.cluster.ip.addresses")
            .withDisplayName("Hosts")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(ConfigSerializerUtil::validateClusterIpAddresses)
            .withDescription("List of IP addresses of nodes in the Scylla cluster that the connector " +
                    "will use to open initial connections to the cluster. " +
                    "In the form of a comma-separated list of pairs <IP>:<PORT>");

    public static final Field TABLE_NAMES = Field.create("scylla.table.names")
            .withDisplayName("Table names")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(ConfigSerializerUtil::validateTableNames)
            .withDescription("List of CDC-enabled table names for connector to read. " +
                    "Provided as a comma-separated list of pairs <keyspace name>.<table name>");

    public static final Field USER = Field.create("scylla.user")
            .withDisplayName("User")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The username to connect to Scylla with. If not set, no authorization is done.");

    public static final Field PASSWORD = Field.create("scylla.password")
            .withDisplayName("Password")
            .withType(ConfigDef.Type.PASSWORD)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The password to connect to Scylla with. If not set, no authorization is done.");

    public static final Field QUERY_TIME_WINDOW_SIZE = Field.create("scylla.query.time.window.size")
            .withDisplayName("Query time window size (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The size of windows queried by the connector. Changes are queried using SELECT statements " +
                    "with time restriction with width defined by this parameter. Value expressed in milliseconds.")
            .withValidation(Field::isNonNegativeInteger)
            .withDefault(30000);

    public static final Field CONFIDENCE_WINDOW_SIZE = Field.create("scylla.confidence.window.size")
            .withDisplayName("Confidence window size (ms)")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The size of the confidence window. It is necessary for the connector to avoid reading too fresh " +
                    "data from the CDC log due to the eventual consistency of Scylla. The problem could appear when a newer write " +
                    "reaches a replica before some older write. For a short period of time, when reading, it " +
                    "is possible for the replica to return only the newer write. The connector mitigates this problem " +
                    "by not reading a window of most recent changes (controlled by this parameter). Value expressed in milliseconds.")
            .withValidation(Field::isNonNegativeInteger)
            .withDefault(30000);

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
                    .type(CLUSTER_IP_ADDRESSES, USER, PASSWORD, LOGICAL_NAME)
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

    protected static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    protected static Field.Set EXPOSED_FIELDS = ALL_FIELDS;

    private final Configuration config;

    protected ScyllaConnectorConfig(Configuration config) {
        super(config, config.getString(LOGICAL_NAME), 0);
        this.config = config;
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public List<InetSocketAddress> getContactPoints() {
        return ConfigSerializerUtil.deserializeClusterIpAddresses(config.getString(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES));
    }

    public Set<TableName> getTableNames() {
        return ConfigSerializerUtil.deserializeTableNames(config.getString(ScyllaConnectorConfig.TABLE_NAMES));
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

    public long getHeartbeatIntervalMs() {
        return config.getInteger(Heartbeat.HEARTBEAT_INTERVAL);
    }

    @Override
    public String getContextName() {
        return "Scylla";
    }

    @Override
    public String getConnectorName() {
        return "scylla";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new ScyllaSourceInfoStructMaker("scylla", Module.version(), this);
    }
}
