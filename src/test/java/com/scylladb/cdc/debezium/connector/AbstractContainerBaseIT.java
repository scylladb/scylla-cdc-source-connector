package com.scylladb.cdc.debezium.connector;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.google.common.flogger.FluentLogger;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(AbstractContainerBaseIT.ContainerLogWatcher.class)
public abstract class AbstractContainerBaseIT {
  private static final int MAX_TABLE_NAME_LENGTH = 48;
  private static final int MAX_CONNECTOR_NAME_LENGTH = 80;

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  static class ContainerLogWatcher implements TestWatcher {
    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
      String testName = context.getDisplayName();
      logContainerDetailsOnFailure(testName, cause);
    }
  }

  protected static void logContainerDetailsOnFailure(String testName, Throwable exception) {
    logger.atSevere().withCause(exception).log("Test failed: %s", testName);

    logger.atInfo().log("Kafka Connect URL: %s", getKafkaConnectUrl());
    logger.atInfo().log("Kafka Connect logs:\n%s", getKafkaConnectLogs());

    try {
      if (KAFKA_PROVIDER == KafkaProvider.APACHE && apacheKafkaContainer != null) {
        logger.atInfo().log(
            "Apache Kafka container logs:\n%s",
            getContainerLogs(apacheKafkaContainer, 2000, 256 * 1024));
      } else if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
        if (confluentKafkaContainer != null) {
          logger.atInfo().log(
              "Confluent Kafka container logs:\n%s",
              getContainerLogs(confluentKafkaContainer, 2000, 256 * 1024));
        }
        if (schemaRegistryContainer != null) {
          logger.atInfo().log(
              "Schema Registry container logs:\n%s",
              getContainerLogs(schemaRegistryContainer, 2000, 256 * 1024));
        }
      }

      // Log ScyllaDB container logs
      if (scyllaDBContainer != null) {
        logger.atInfo().log(
            "ScyllaDB container logs:\n%s", getContainerLogs(scyllaDBContainer, 2000, 256 * 1024));
      }
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Failed to collect container logs on test failure");
    }
  }

  /** Enum representing supported Kafka providers. */
  public enum KafkaProvider {
    APACHE("apache"),
    CONFLUENT("confluent");

    private final String value;

    KafkaProvider(String value) {
      this.value = value;
    }

    public static KafkaProvider fromString(String value) {
      if (value == null) {
        return null;
      }

      for (KafkaProvider provider : KafkaProvider.values()) {
        if (provider.value.equalsIgnoreCase(value)) {
          return provider;
        }
      }
      throw new IllegalArgumentException("Unknown Kafka provider: " + value);
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /** Enum representing Kafka Connect deployment modes. */
  public enum KafkaConnectMode {
    DISTRIBUTED("distributed"),
    STANDALONE("standalone");

    private final String value;

    KafkaConnectMode(String value) {
      this.value = value;
    }

    public static KafkaConnectMode fromString(String value) {
      if (value == null) {
        return null;
      }

      for (KafkaConnectMode mode : KafkaConnectMode.values()) {
        if (mode.value.equalsIgnoreCase(value)) {
          return mode;
        }
      }
      throw new IllegalArgumentException("Unknown Kafka Connect mode: " + value);
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /**
   * Class representing semantic versioning for Kafka. Supports versions in format:
   * major.minor.patch[-label]
   */
  public static class KafkaVersion implements Comparable<KafkaVersion> {
    private final int major;
    private final int minor;
    private final int patch;
    private final String label;

    public KafkaVersion(int major, int minor, int patch) {
      this(major, minor, patch, null);
    }

    public KafkaVersion(int major, int minor, int patch, String label) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.label = label;
    }

    public KafkaVersion(String version) {
      if (version == null || version.isEmpty()) {
        throw new IllegalArgumentException("Version cannot be null or empty");
      }

      // Check if version has a label part (e.g., "4.1.0-rc1")
      String versionPart = version;
      String labelPart = null;

      int labelSeparatorIndex = version.indexOf('-');
      if (labelSeparatorIndex > 0) {
        versionPart = version.substring(0, labelSeparatorIndex);
        labelPart = version.substring(labelSeparatorIndex + 1);
      }

      String[] parts = versionPart.split("\\.");
      if (parts.length != 3) {
        throw new IllegalArgumentException(
            "Invalid version format. Expected: major.minor.patch[-label]");
      }

      try {
        this.major = Integer.parseInt(parts[0]);
        this.minor = Integer.parseInt(parts[1]);
        this.patch = Integer.parseInt(parts[2]);
        this.label = labelPart;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid version format. Version components must be integers.", e);
      }
    }

    public int getMajor() {
      return major;
    }

    public int getMinor() {
      return minor;
    }

    public int getPatch() {
      return patch;
    }

    public String getLabel() {
      return label;
    }

    public boolean hasLabel() {
      return label != null && !label.isEmpty();
    }

    /**
     * Compares this version to another KafkaVersion. Returns a negative integer, zero, or a
     * positive integer as this version is less than, equal to, or greater than the specified
     * version. Ignores the label part in comparisons.
     */
    public int compareTo(KafkaVersion other) {
      if (this.major != other.major) {
        return Integer.compare(this.major, other.major);
      }
      if (this.minor != other.minor) {
        return Integer.compare(this.minor, other.minor);
      }
      return Integer.compare(this.patch, other.patch);
    }

    @Override
    public String toString() {
      if (hasLabel()) {
        return major + "." + minor + "." + patch + "-" + label;
      }
      return major + "." + minor + "." + patch;
    }
  }

  /** The default Kafka provider when system property is not defined */
  private static final String DEFAULT_KAFKA_PROVIDER = "apache";

  /** The default Kafka version when system property is not defined (Apache provider). */
  private static final String DEFAULT_APACHE_PROVIDER_IMAGE_VERSION = "4.0.0";

  /** The default Kafka version when system property is not defined (Confluent provider). */
  private static final String DEFAULT_CONFLUENT_PROVIDER_IMAGE_VERSION = "7.5.0";

  /** The default Kafka Connect mode when system property is not defined */
  private static final String DEFAULT_KAFKA_CONNECT_MODE = "distributed";

  /** The default ScyllaDB version when system property is not defined */
  private static final String DEFAULT_SCYLLA_VERSION = "6.2";

  /**
   * The Kafka provider, read from the "it.kafka.provider" system property. Expected values:
   * "apache" or "confluent". Defaults to "apache" if system property is not defined.
   */
  public static final KafkaProvider KAFKA_PROVIDER =
      KafkaProvider.fromString(System.getProperty("it.kafka.provider", DEFAULT_KAFKA_PROVIDER));

  /**
   * The Kafka version, read from the "it.provider.image.version" system property. Expected format:
   * major.minor.patch[-label] (e.g. "2.8.1" or "4.1.0-rc1"). Defaults to the provider-specific
   * version if the system property is not defined.
   */
  public static final KafkaVersion PROVIDER_IMAGE_VERSION =
      new KafkaVersion(
          System.getProperty("it.provider.image.version", defaultProviderImageVersion()));

  private static String defaultProviderImageVersion() {
    if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
      return DEFAULT_CONFLUENT_PROVIDER_IMAGE_VERSION;
    }
    return DEFAULT_APACHE_PROVIDER_IMAGE_VERSION;
  }

  /**
   * The Kafka Connect deployment mode, read from the "it.kafka.connect.mode" system property.
   * Expected values: "distributed" or "standalone". Defaults to "distributed" if system property is
   * not defined.
   */
  public static final KafkaConnectMode KAFKA_CONNECT_MODE =
      KafkaConnectMode.fromString(
          System.getProperty("it.kafka.connect.mode", DEFAULT_KAFKA_CONNECT_MODE));

  /**
   * The ScyllaDB version, read from the "it.scylla.version" system property. Defaults to "6.2" if
   * system property is not defined.
   */
  public static final String SCYLLA_VERSION =
      System.getProperty("it.scylla.version", DEFAULT_SCYLLA_VERSION);

  /**
   * Builds the ScyllaDB server command used by the container. Includes developer/overprovisioned
   * defaults and allows reducing log verbosity via system properties:
   * -Dit.scylla.log.default=warn|error|info (default: warn)
   * -Dit.scylla.log.modules="cql_server=warn,sstable=error" (comma or colon separated)
   *
   * @return the complete ScyllaDB server command string with configured logging options
   */
  private static String buildScyllaCommand() {
    // Default to WARN to reduce verbosity; keep essential module(s) at INFO
    // so readiness logs are still emitted.
    String defaultLevel = System.getProperty("it.scylla.log.default", "warn");
    // If caller did not provide module overrides, ensure init=info for readiness.
    String modules = System.getProperty("it.scylla.log.modules", "init=info");

    StringBuilder cmd =
        new StringBuilder("--developer-mode=1 --overprovisioned=1 --default-log-level=")
            .append(defaultLevel);

    if (!modules.trim().isEmpty()) {
      String normalized = modules.trim().replace(",", ":").replaceAll("\\s+", "");
      cmd.append(" --logger-log-level ").append(normalized);
    }

    return cmd.toString();
  }

  /** The Network shared by all containers in this setup */
  protected static final Network NETWORK = Network.newNetwork();

  /** Default port for Kafka */
  protected static final int KAFKA_PORT = 9092;

  /** Default port for Schema Registry */
  protected static final int SCHEMA_REGISTRY_PORT = 8081;

  /** Default port for Kafka Connect */
  protected static final int KAFKA_CONNECT_PORT = 8083;

  /** Default port for ScyllaDB */
  protected static final int SCYLLA_PORT = 9042;

  /** Container for Apache Kafka */
  protected static KafkaContainer apacheKafkaContainer;

  /** Container for Confluent Kafka */
  protected static ConfluentKafkaContainer confluentKafkaContainer;

  /** Container for Confluent Schema Registry */
  protected static GenericContainer<?> schemaRegistryContainer;

  /** Container for Confluent Kafka Connect */
  protected static GenericContainer<?> kafkaConnectContainer;

  /** Container for ScyllaDB */
  protected static ScyllaDBContainer scyllaDBContainer;

  /**
   * Start the Kafka infrastructure containers based on the provider configuration. For Apache
   * provider, only one container is started. For Confluent provider, multiple containers are
   * started (Kafka, Schema Registry, Kafka Connect).
   *
   * @return Map of container information including connection details
   */
  public static synchronized Map<String, String> startKafkaInfrastructure() {
    Map<String, String> result = new HashMap<>();

    if (KAFKA_PROVIDER == KafkaProvider.APACHE) {
      startApacheKafka();
      result.put("bootstrap.servers", apacheKafkaContainer.getBootstrapServers());
    } else if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
      startConfluentInfrastructure();
      result.put(
          "bootstrap.servers", "localhost:" + confluentKafkaContainer.getMappedPort(KAFKA_PORT));
      result.put(
          "schema.registry.url",
          "http://localhost:" + schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT));
      if (KAFKA_CONNECT_MODE.equals(KafkaConnectMode.DISTRIBUTED))
        result.put(
            "connect.url",
            "http://localhost:" + kafkaConnectContainer.getMappedPort(KAFKA_CONNECT_PORT));
    } else {
      throw new IllegalStateException("Unsupported Kafka provider: " + KAFKA_PROVIDER);
    }

    return result;
  }

  /** Start an Apache Kafka container */
  private static void startApacheKafka() {
    if (apacheKafkaContainer != null && apacheKafkaContainer.isRunning()) {
      return;
    }

    // Determine the right Apache Kafka version based on the configuration
    String imageVersion = PROVIDER_IMAGE_VERSION.toString();

    apacheKafkaContainer =
        new KafkaContainer(DockerImageName.parse("apache/kafka:" + imageVersion))
            .withNetwork(NETWORK)
            .withExposedPorts(KAFKA_PORT, KAFKA_CONNECT_PORT)
            .withListener("broker:19092")
            .withFileSystemBind("target/components/packages/", "/opt/custom-connectors")
            .withStartupTimeout(Duration.ofMinutes(5));

    apacheKafkaContainer.start();

    // Start Kafka Connect in the appropriate mode inside the container
    try {
      String connectScript;
      String connectConfigPath;

      if (KAFKA_CONNECT_MODE == KafkaConnectMode.STANDALONE) {
        connectScript = "/opt/kafka/bin/connect-standalone.sh";
        connectConfigPath = "/tmp/connect-standalone.properties";

        // Create custom standalone properties
        String standaloneProperties =
            "bootstrap.servers=broker:19092\n"
                + "key.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "value.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "key.converter.schemas.enable=true\n"
                + "value.converter.schemas.enable=true\n"
                + "offset.storage.file.filename=/tmp/connect.offsets\n"
                + "offset.flush.interval.ms=10000\n"
                + "plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,/opt/custom-connectors\n";

        // Write custom standalone properties to container
        apacheKafkaContainer.execInContainer(
            "sh", "-c", "echo '" + standaloneProperties + "' > " + connectConfigPath);

      } else {
        connectScript = "/opt/kafka/bin/connect-distributed.sh";
        connectConfigPath = "/tmp/connect-distributed.properties";

        // Create custom distributed properties
        String distributedProperties =
            "bootstrap.servers=broker:19092\n"
                + "group.id=connect-cluster\n"
                + "key.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "value.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "key.converter.schemas.enable=true\n"
                + "value.converter.schemas.enable=true\n"
                + "offset.storage.topic=connect-offsets\n"
                + "offset.storage.replication.factor=1\n"
                + "config.storage.topic=connect-configs\n"
                + "config.storage.replication.factor=1\n"
                + "status.storage.topic=connect-status\n"
                + "status.storage.replication.factor=1\n"
                + "offset.flush.interval.ms=10000\n"
                + "plugin.path=/usr/local/share/java,/usr/local/share/kafka/plugins,/opt/custom-connectors\n";

        // Write custom distributed properties to container
        apacheKafkaContainer.execInContainer(
            "sh", "-c", "echo '" + distributedProperties + "' > " + connectConfigPath);
      }

      // Execute Kafka Connect in the background
      String[] command = {
        "sh",
        "-c",
        String.format(
            "nohup %s %s > /tmp/kafka-connect.log 2>&1 &", connectScript, connectConfigPath)
      };

      apacheKafkaContainer.execInContainer(command);

      // Wait for Kafka Connect to be ready by polling the REST API
      waitForKafkaConnectReady(apacheKafkaContainer);

    } catch (Exception e) {
      throw new RuntimeException("Failed to start Kafka Connect in Apache Kafka container", e);
    }
  }

  /**
   * Waits for Kafka Connect to be ready by repeatedly polling the /connectors endpoint until it
   * returns an empty array, indicating the REST API is operational.
   */
  private static void waitForKafkaConnectReady(GenericContainer<?> connectContainer) {
    int maxAttempts = 60; // 60 attempts with 1 second intervals = 1 minute timeout
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        // Get the mapped port for Kafka Connect on the host machine
        int kafkaConnectMappedPort = connectContainer.getMappedPort(KAFKA_CONNECT_PORT);
        String connectUrl = "http://localhost:" + kafkaConnectMappedPort + "/connectors";

        // Use Java HTTP API to check if Kafka Connect is ready
        URL url = new URL(connectUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/json");
        conn.setConnectTimeout(5000); // 5 second connection timeout
        conn.setReadTimeout(5000); // 5 second read timeout

        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
          // Read the response content to verify it's an empty JSON array
          StringBuilder response = new StringBuilder();
          try (BufferedReader in =
              new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
              response.append(inputLine);
            }
          }

          String responseContent = response.toString().trim();
          if ("[]".equals(responseContent)) {
            // Kafka Connect is ready and returns empty connectors list
            logger.atInfo().log("Kafka Connect is ready at %s", connectUrl);
            conn.disconnect();
            return;
          }
        }
        conn.disconnect();

      } catch (Exception e) {
        // Ignore exceptions during health check attempts (connection refused, etc.)
      }

      attempt++;
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    throw new RuntimeException(
        "Kafka Connect did not become ready within " + maxAttempts + " seconds");
  }

  /** Start the Confluent infrastructure (Kafka, Schema Registry, Kafka Connect) */
  private static void startConfluentInfrastructure() {
    if (confluentKafkaContainer != null && confluentKafkaContainer.isRunning()) {
      return;
    }

    String imageVersion = PROVIDER_IMAGE_VERSION.toString();
    String cpPrefix = "confluentinc/cp-";

    confluentKafkaContainer =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:" + imageVersion)
            .withNetwork(NETWORK)
            .withNetworkAliases("broker")
            .withListener("broker:19092")
            .withExposedPorts(KAFKA_PORT, KAFKA_CONNECT_PORT)
            .withFileSystemBind("target/components/packages/", "/opt/custom-connectors")
            .withStartupTimeout(Duration.ofMinutes(5));

    schemaRegistryContainer =
        new GenericContainer<>(DockerImageName.parse(cpPrefix + "schema-registry:" + imageVersion))
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(SCHEMA_REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:19092")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .dependsOn(confluentKafkaContainer)
            .withStartupTimeout(Duration.ofMinutes(5));

    if (KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED) {
      kafkaConnectContainer =
          new GenericContainer<>(DockerImageName.parse(cpPrefix + "kafka-connect:" + imageVersion))
              .dependsOn(confluentKafkaContainer, schemaRegistryContainer)
              .withNetwork(NETWORK)
              .withFileSystemBind("target/components/packages/", "/opt/custom-connectors")
              .withNetworkAliases("kafka-connect")
              .withExposedPorts(KAFKA_CONNECT_PORT)
              .withEnv("CONNECT_BOOTSTRAP_SERVERS", "broker:19092")
              .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
              .withEnv("CONNECT_GROUP_ID", "kafka-connect")
              .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "docker-connect-configs")
              .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
              .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "10000")
              .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "docker-connect-offsets")
              .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
              .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "docker-connect-status")
              .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
              .withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
              .withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
              .withEnv(
                  "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                  "http://schema-registry:" + SCHEMA_REGISTRY_PORT)
              .withEnv(
                  "CONNECT_PLUGIN_PATH",
                  "/usr/share/java,/usr/share/confluent-hub-components,/kafka/connect/plugins/,/opt/custom-connectors")
              .withStartupTimeout(Duration.ofMinutes(5));

      Startables.deepStart(confluentKafkaContainer, kafkaConnectContainer, schemaRegistryContainer)
          .join();
    } else {
      // Standalone mode: start only Kafka and Schema Registry containers, then start Connect inside
      // Kafka container
      Startables.deepStart(confluentKafkaContainer, schemaRegistryContainer).join();

      // Start Kafka Connect in standalone mode inside the Confluent Kafka container
      try {
        String connectScript = "/usr/bin/connect-standalone";
        String connectConfigPath = "/tmp/connect-standalone.properties";

        // Create custom standalone properties for Confluent
        String standaloneProperties =
            "bootstrap.servers=broker:19092\n"
                + "key.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "value.converter=org.apache.kafka.connect.json.JsonConverter\n"
                + "key.converter.schemas.enable=true\n"
                + "value.converter.schemas.enable=true\n"
                + "offset.storage.file.filename=/tmp/connect.offsets\n"
                + "offset.flush.interval.ms=10000\n"
                + "plugin.path=/usr/share/java,/usr/share/confluent-hub-components,/kafka/connect/plugins/,/opt/custom-connectors\n";

        // Write custom standalone properties to container
        confluentKafkaContainer.execInContainer(
            "sh", "-c", "echo '" + standaloneProperties + "' > " + connectConfigPath);

        // Execute Kafka Connect in the background
        String[] command = {
          "sh",
          "-c",
          String.format(
              "nohup %s %s > /tmp/kafka-connect.log 2>&1 &", connectScript, connectConfigPath)
        };

        confluentKafkaContainer.execInContainer(command);

        // Wait for Kafka Connect to be ready by polling the REST API
        waitForKafkaConnectReady(confluentKafkaContainer);

      } catch (Exception e) {
        throw new RuntimeException("Failed to start Kafka Connect in Confluent Kafka container", e);
      }
    }
  }

  /** Stop all running containers */
  public static synchronized void stopKafkaInfrastructure() {
    if (apacheKafkaContainer != null && apacheKafkaContainer.isRunning()) {
      apacheKafkaContainer.stop();
    }

    if (kafkaConnectContainer != null && kafkaConnectContainer.isRunning()) {
      kafkaConnectContainer.stop();
    }

    if (schemaRegistryContainer != null && schemaRegistryContainer.isRunning()) {
      schemaRegistryContainer.stop();
    }

    if (confluentKafkaContainer != null && confluentKafkaContainer.isRunning()) {
      confluentKafkaContainer.stop();
    }
  }

  /**
   * Gets the Kafka Connect URL from the running container based on the current mode. For
   * distributed mode with Confluent provider, uses the separate Kafka Connect container. For all
   * other cases (Apache distributed/standalone, Confluent standalone), uses the main Kafka
   * container.
   *
   * @return The Kafka Connect URL, or null if not running
   */
  public static String getKafkaConnectUrl() {
    if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT
        && KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED) {
      // Confluent distributed mode uses separate Kafka Connect container
      if (kafkaConnectContainer == null || !kafkaConnectContainer.isRunning()) {
        return null;
      }
      return "http://localhost:" + kafkaConnectContainer.getMappedPort(KAFKA_CONNECT_PORT);
    } else {
      // All other modes use Kafka Connect running inside the main Kafka container
      GenericContainer<?> kafkaContainer = null;
      if (KAFKA_PROVIDER == KafkaProvider.APACHE) {
        kafkaContainer = apacheKafkaContainer;
      } else if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
        kafkaContainer = confluentKafkaContainer;
      }

      if (kafkaContainer == null || !kafkaContainer.isRunning()) {
        return null;
      }
      return "http://localhost:" + kafkaContainer.getMappedPort(KAFKA_CONNECT_PORT);
    }
  }

  /**
   * Gets the Schema Registry URL from the running container.
   *
   * @return The Schema Registry URL, or null if not running
   */
  public static String getSchemaRegistryUrl() {
    if (schemaRegistryContainer == null || !schemaRegistryContainer.isRunning()) {
      return null;
    }
    return "http://localhost:" + schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT);
  }

  /** Start a ScyllaDB container */
  public static synchronized void startScyllaDB() {
    if (scyllaDBContainer != null && scyllaDBContainer.isRunning()) {
      return;
    }

    var scyllaCommand = buildScyllaCommand();
    scyllaDBContainer =
        new ScyllaDBContainer("scylladb/scylla:" + SCYLLA_VERSION)
            .withNetwork(NETWORK)
            .withNetworkAliases("scylla")
            .withExposedPorts(SCYLLA_PORT)
            .withCommand(scyllaCommand) // Configure Scylla logging verbosity via command flags
            .withStartupTimeout(Duration.ofMinutes(5));

    logger.atInfo().log("Using Scylla command: %s", scyllaCommand);

    scyllaDBContainer.start();

    logger.atInfo().log(
        "ScyllaDB container started. Listening on port %d",
        scyllaDBContainer.getMappedPort(SCYLLA_PORT));
  }

  /**
   * Gets the Kafka bootstrap servers URL from the running container.
   *
   * @return The Kafka bootstrap servers URL, or null if not running
   */
  public static String getKafkaBootstrapServers() {
    if (KAFKA_PROVIDER == KafkaProvider.APACHE) {
      if (apacheKafkaContainer == null || !apacheKafkaContainer.isRunning()) {
        return null;
      }
      return apacheKafkaContainer.getBootstrapServers();
    } else if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
      if (confluentKafkaContainer == null || !confluentKafkaContainer.isRunning()) {
        return null;
      }
      return confluentKafkaContainer.getBootstrapServers();
    }
    return null;
  }

  /**
   * Gets the Kafka Connect logs from the appropriate container based on the provider and mode.
   *
   * @return The Kafka Connect logs as a string, or null if not available
   */
  public static String getKafkaConnectLogs() {
    try {
      if (KAFKA_PROVIDER == KafkaProvider.APACHE) {
        // For Apache Kafka, Connect runs inside the main Kafka container
        if (apacheKafkaContainer == null || !apacheKafkaContainer.isRunning()) {
          return null;
        }

        // Kafka Connect logs are written to /tmp/kafka-connect.log
        return apacheKafkaContainer
            .execInContainer("sh", "-c", "tail -n 2000 /tmp/kafka-connect.log || true")
            .getStdout();

      } else if (KAFKA_PROVIDER == KafkaProvider.CONFLUENT) {
        if (KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED) {
          // For Confluent distributed mode, Connect runs in a separate container
          if (kafkaConnectContainer == null || !kafkaConnectContainer.isRunning()) {
            return null;
          }

          // Get logs from the dedicated Kafka Connect container
          return getContainerLogs(kafkaConnectContainer, 2000, 256 * 1024);

        } else {
          // For Confluent standalone mode, Connect runs inside the Kafka container
          if (confluentKafkaContainer == null || !confluentKafkaContainer.isRunning()) {
            return null;
          }

          // Kafka Connect logs are written to /tmp/kafka-connect.log
          return confluentKafkaContainer
              .execInContainer("sh", "-c", "tail -n 2000 /tmp/kafka-connect.log || true")
              .getStdout();
        }
      }

      return null;

    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Failed to retrieve Kafka Connect logs");
      return "Error retrieving logs: " + e.getMessage();
    }
  }

  static {
    // Start the Kafka infrastructure containers
    logger.atInfo().log(
        "Starting Kafka infrastructure with provider: %s, version: %s, connect mode: %s",
        KAFKA_PROVIDER, PROVIDER_IMAGE_VERSION, KAFKA_CONNECT_MODE);
    startKafkaInfrastructure();
    // Start the ScyllaDB container
    logger.atInfo().log("Starting ScyllaDB container with version: %s", SCYLLA_VERSION);
    startScyllaDB();
  }

  private static String getContainerLogs(
      GenericContainer<?> container, int tailLines, int maxChars) {
    if (container == null || !container.isRunning()) {
      return null;
    }
    StringBuilder logs = new StringBuilder();
    try {
      ResultCallback.Adapter<Frame> callback =
          new ResultCallback.Adapter<>() {
            @Override
            public void onNext(Frame frame) {
              if (logs.length() >= maxChars) {
                return;
              }
              String payload = new String(frame.getPayload(), StandardCharsets.UTF_8);
              int remaining = maxChars - logs.length();
              if (payload.length() > remaining) {
                logs.append(payload, 0, remaining);
              } else {
                logs.append(payload);
              }
            }
          };

      container
          .getDockerClient()
          .logContainerCmd(container.getContainerId())
          .withStdOut(true)
          .withStdErr(true)
          .withTail(tailLines)
          .exec(callback)
          .awaitCompletion(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.atWarning().withCause(e).log("Failed to retrieve container logs");
      return "Error retrieving logs: " + e.getMessage();
    }

    return logs.toString();
  }

  protected static String connectorName(TestInfo testInfo) {
    String testMethodName = testInfo.getTestMethod().orElseThrow().getName();
    String className = simplifyName(testInfo.getTestClass().orElseThrow().getName());
    return trimWithHash(className + "_" + testMethodName, MAX_CONNECTOR_NAME_LENGTH);
  }

  protected static String tableName(TestInfo testInfo) {
    return trimWithHash(testInfo.getTestMethod().orElseThrow().getName(), MAX_TABLE_NAME_LENGTH)
        .toLowerCase(Locale.ROOT);
  }

  protected static String keyspaceName(TestInfo testInfo) {
    return trimWithHash(
            simplifyName(testInfo.getTestClass().orElseThrow().getName()), MAX_TABLE_NAME_LENGTH)
        .toLowerCase(Locale.ROOT);
  }

  protected static String keyspaceTableName(TestInfo testInfo) {
    return keyspaceName(testInfo) + "." + tableName(testInfo);
  }

  public static String simplifyName(String input) {
    // 1) Take only the part after the last dot
    String afterLastDot = input.substring(input.lastIndexOf('.') + 1);

    // 2) Remove nested class separators without dropping the outer class name.
    return afterLastDot.replace("$", "");
  }

  static String trimWithHash(String value, int maxLength) {
    if (value.length() <= maxLength) {
      return value;
    }
    String hash = Integer.toHexString(value.hashCode());
    int maxPrefixLength = maxLength - hash.length() - 1;
    if (maxPrefixLength <= 0) {
      return value.substring(0, maxLength);
    }
    return value.substring(0, maxPrefixLength) + "_" + hash;
  }
}
