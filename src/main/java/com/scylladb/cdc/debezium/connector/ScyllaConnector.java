package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.scylladb.cdc.cql.driver3.Driver3MasterCQL;
import com.scylladb.cdc.cql.driver3.Driver3Session;
import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.master.Master;
import com.scylladb.cdc.model.master.MasterConfiguration;
import io.debezium.config.Configuration;
import io.debezium.util.Threads;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public ScyllaConnector() {}

  @Override
  public void start(Map<String, String> props) {
    final Configuration config = Configuration.from(props);
    final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
    this.config = config;

    // Validate that ONLY_UPDATED mode is not configured (not yet implemented)
    validateOnlyUpdatedNotConfigured(connectorConfig);

    // Start master, which will watch for
    // new generations.
    this.startMaster(connectorConfig);
  }

  /**
   * Validates that the ONLY_UPDATED mode is not configured. This mode is reserved for future
   * implementation and will throw an exception if used.
   *
   * @param connectorConfig the connector configuration to validate
   * @throws IllegalArgumentException if ONLY_UPDATED mode is configured
   */
  private void validateOnlyUpdatedNotConfigured(ScyllaConnectorConfig connectorConfig) {
    if (connectorConfig.getCdcIncludeBefore() == CdcIncludeMode.ONLY_UPDATED) {
      throw new IllegalArgumentException(
          "cdc.include.before=only-updated is not yet implemented. "
              + "Please use 'none' or 'full' instead.");
    }
    if (connectorConfig.getCdcIncludeAfter() == CdcIncludeMode.ONLY_UPDATED) {
      throw new IllegalArgumentException(
          "cdc.include.after=only-updated is not yet implemented. "
              + "Please use 'none' or 'full' instead.");
    }
  }

  private Master buildMaster(ScyllaConnectorConfig connectorConfig) {
    this.masterSession = new ScyllaSessionBuilder(connectorConfig).build();
    Driver3MasterCQL cql = new Driver3MasterCQL(masterSession);
    this.masterTransport = new ScyllaMasterTransport(context(), connectorConfig);
    Set<TableName> tableNames = connectorConfig.getTableNames();
    MasterConfiguration masterConfiguration =
        MasterConfiguration.builder()
            .withTransport(masterTransport)
            .withCQL(cql)
            .addTables(tableNames)
            .build();
    return new Master(masterConfiguration);
  }

  private void startMaster(ScyllaConnectorConfig connectorConfig) {
    Master master = buildMaster(connectorConfig);

    this.masterExecutor =
        Threads.newSingleThreadExecutor(
            ScyllaConnector.class,
            connectorConfig.getLogicalName(),
            "scylla-cdc-java-master-executor");
    this.masterExecutor.execute(
        () -> {
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
    return workerConfigs.stream()
        .map(
            c ->
                config
                    .edit()
                    .with(ScyllaConnectorConfig.WORKER_CONFIG, c)
                    .withDefault(
                        ScyllaConnectorConfig.CUSTOM_HEARTBEAT_INTERVAL,
                        ScyllaConnectorConfig.CUSTOM_HEARTBEAT_INTERVAL.defaultValue())
                    .build()
                    .asMap())
        .collect(Collectors.toList());
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

    ConfigValue clusterIpAddressesConfig =
        results.get(ScyllaConnectorConfig.CLUSTER_IP_ADDRESSES.name());
    ConfigValue tableNamesConfig = results.get(ScyllaConnectorConfig.TABLE_NAMES.name());

    ConfigValue userConfig = results.get(ScyllaConnectorConfig.USER.name());
    ConfigValue passwordConfig = results.get(ScyllaConnectorConfig.PASSWORD.name());
    ConfigValue cdcIncludeBeforeConfig =
        results.get(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE.name());
    ConfigValue cdcIncludeAfterConfig = results.get(ScyllaConnectorConfig.CDC_INCLUDE_AFTER.name());

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

        // Validate CDC table options (preimage/postimage) if required by config
        validateCdcTableOptions(
            connectorConfig, tableNamesConfig, cdcIncludeBeforeConfig, cdcIncludeAfterConfig);
      } catch (Exception ex) {
        // TODO - catch specific exceptions, for example authentication error
        // should add error message to user, password fields instead of
        // clusterIpAddressesConfig.
        clusterIpAddressesConfig.addErrorMessage(
            "Unable to connect to Scylla cluster: " + ex.getMessage());
      } finally {
        // Stop the session created for Master.
        this.stop();
      }
    }

    return new Config(new ArrayList<>(results.values()));
  }

  /**
   * Validates that table CDC options match the connector configuration requirements.
   *
   * <p>If cdc.include.before=full, tables must have preimage enabled. If cdc.include.after=full,
   * tables must have postimage enabled. Missing tables are logged but don't cause validation
   * failure (connector will wait for them).
   */
  private void validateCdcTableOptions(
      ScyllaConnectorConfig connectorConfig,
      ConfigValue tableNamesConfig,
      ConfigValue cdcIncludeBeforeConfig,
      ConfigValue cdcIncludeAfterConfig) {

    CdcIncludeMode includeBefore = connectorConfig.getCdcIncludeBefore();
    CdcIncludeMode includeAfter = connectorConfig.getCdcIncludeAfter();

    // Skip validation if both are 'none'
    if (includeBefore == CdcIncludeMode.NONE && includeAfter == CdcIncludeMode.NONE) {
      return;
    }

    Set<TableName> tableNames = connectorConfig.getTableNames();
    if (tableNames == null || tableNames.isEmpty()) {
      return;
    }

    // Build a separate cluster connection for metadata queries
    Cluster.Builder clusterBuilder = Cluster.builder();
    for (InetSocketAddress contactPoint : connectorConfig.getContactPoints()) {
      clusterBuilder.addContactPoint(contactPoint.getHostString()).withPort(contactPoint.getPort());
    }
    if (connectorConfig.getUser() != null && connectorConfig.getPassword() != null) {
      clusterBuilder.withCredentials(connectorConfig.getUser(), connectorConfig.getPassword());
    }

    try (Cluster cluster = clusterBuilder.build()) {
      CdcTableOptionsValidator validator =
          new CdcTableOptionsValidator(includeBefore, includeAfter);
      CdcTableOptionsValidator.ValidationResult result = validator.validate(cluster, tableNames);

      // Report errors for tables with wrong CDC configuration
      if (result.hasErrors()) {
        for (String error : result.getErrors()) {
          // Add errors to the most relevant config field
          if (error.contains("preimage") && includeBefore == CdcIncludeMode.FULL) {
            cdcIncludeBeforeConfig.addErrorMessage(error);
          } else if (error.contains("postimage") && includeAfter == CdcIncludeMode.FULL) {
            cdcIncludeAfterConfig.addErrorMessage(error);
          } else {
            tableNamesConfig.addErrorMessage(error);
          }
        }
      }

      // Log missing tables (they don't cause validation failure - connector will wait for them)
      if (result.hasMissingTables()) {
        logger.warn(
            "The following tables do not exist yet and will be waited for: {}",
            result.getMissingTables());
      }
    } catch (Exception ex) {
      logger.warn("Failed to validate CDC table options: {}", ex.getMessage());
    }
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
