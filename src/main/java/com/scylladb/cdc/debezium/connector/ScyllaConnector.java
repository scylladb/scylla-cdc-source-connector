package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.MasterConfiguration;
import com.scylladb.cdc.model.master.Master;
import io.debezium.config.Configuration;
import io.debezium.util.Threads;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ScyllaConnector extends SourceConnector {
    static {
        // Route Flogger logs from scylla-cdc-java library
        // to log4j.
        System.setProperty(
                "flogger.backend_factory",
                "com.google.common.flogger.backend.log4j.Log4jBackendFactory#getInstance");
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Configuration config;

    // Used by background generation master.
    private ScyllaMasterTransport masterTransport;
    private ExecutorService masterExecutor;
    private Driver3Session masterSession;

    public ScyllaConnector() {
    }

    @Override
    public void start(Map<String, String> props) {
        final Configuration config = Configuration.from(props);
        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
        this.config = config;

        // Start master, which will watch for
        // new generations.
        this.startMaster(connectorConfig);
    }

    private Master buildMaster(ScyllaConnectorConfig connectorConfig) {
        this.masterSession = new ScyllaSessionBuilder(connectorConfig).build();
        Driver3MasterCQL cql = new Driver3MasterCQL(masterSession);
        this.masterTransport = new ScyllaMasterTransport(context(), connectorConfig);
        Set<TableName> tableNames = connectorConfig.getTableNames();
        MasterConfiguration masterConfiguration = MasterConfiguration.builder()
                .withTransport(masterTransport)
                .withCQL(cql)
                .addTables(tableNames)
                .build();
        return new Master(masterConfiguration);
    }

    private void startMaster(ScyllaConnectorConfig connectorConfig) {
        Master master = buildMaster(connectorConfig);

        this.masterExecutor = Threads.newSingleThreadExecutor(ScyllaConnector.class, connectorConfig.getLogicalName(),
                "scylla-cdc-java-master-executor");
        this.masterExecutor.execute(() -> {
            master.run();
            logger.info("scylla-cdc-java library master gracefully finished.");
        });
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ScyllaConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<TaskId, SortedSet<StreamId>> tasks = masterTransport.getWorkerConfigurations();
        List<String> workerConfigs = new TaskConfigBuilder(tasks).buildTaskConfigs(maxTasks);
        return workerConfigs.stream().map(c -> config.edit()
                .with(ScyllaConnectorConfig.WORKER_CONFIG, c)
                .withDefault(ScyllaConnectorConfig.CUSTOM_HEARTBEAT_INTERVAL, ScyllaConnectorConfig.CUSTOM_HEARTBEAT_INTERVAL.defaultValue())
                .build().asMap()).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        // Clear interrupt flag so the graceful termination is always attempted.
        Thread.interrupted();

        if (this.masterExecutor != null) {
            this.masterExecutor.shutdownNow();
        }
        if (this.masterSession != null) {
            this.masterSession.close();
        }
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Configuration config = Configuration.from(connectorConfigs);
        Map<String, ConfigValue> results = config.validate(ScyllaConnectorConfig.EXPOSED_FIELDS);

        ConfigValue clusterIpAddressesConfig = results.get(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES.name());

        ConfigValue userConfig = results.get(ScyllaConnectorConfig.USER.name());
        ConfigValue passwordConfig = results.get(ScyllaConnectorConfig.PASSWORD.name());

        // Do a trial connection, if no errors:
        boolean noErrors = results.values().stream().allMatch(c -> c.errorMessages().isEmpty());
        if (noErrors) {
            final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);

            if (connectorConfig.getUser() == null && connectorConfig.getPassword() != null) {
                userConfig.addErrorMessage("Username is not set while password was set.");
            } else if (connectorConfig.getUser() != null && connectorConfig.getPassword() == null) {
                passwordConfig.addErrorMessage("Password is not set while username was set.");
            }

            try {
                Master master = buildMaster(connectorConfig);

                Optional<Throwable> validation = master.validate();
                validation.ifPresent(error -> clusterIpAddressesConfig.addErrorMessage(error.getMessage()));
            } catch (Exception ex) {
                // TODO - catch specific exceptions, for example authentication error
                // should add error message to user, password fields instead of
                // clusterIpAddressesConfig.
                clusterIpAddressesConfig.addErrorMessage("Unable to connect to Scylla cluster: " + ex.getMessage());
            } finally {
                // Stop the session created for Master.
                this.stop();
            }
        }

        return new Config(new ArrayList<>(results.values()));
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ConfigDef config() {
        return ScyllaConnectorConfig.configDef();
    }
}
