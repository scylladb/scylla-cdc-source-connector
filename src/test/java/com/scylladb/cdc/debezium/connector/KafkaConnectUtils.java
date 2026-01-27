package com.scylladb.cdc.debezium.connector;

import com.google.common.flogger.FluentLogger;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

/** Utility class for Kafka Connect operations. */
public final class KafkaConnectUtils {
  protected static final int CONSUMER_TIMEOUT = 65 * 1000;

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  // Retry configuration with exponential backoff
  private static final int MAX_RETRIES = 10;
  private static final long INITIAL_BACKOFF_MS = 500; // 0.5 seconds
  private static final long MAX_BACKOFF_MS = 60000; // 1 minute

  private KafkaConnectUtils() {
    throw new UnsupportedOperationException("This utility class cannot be instantiated");
  }

  /** Result of an HTTP request containing response code and body. */
  private static class HttpResult {
    final int code;
    final String body;

    HttpResult(int code, String body) {
      this.code = code;
      this.body = body;
    }
  }

  /**
   * Executes an HTTP request to the Kafka Connect REST API.
   *
   * @param method The HTTP method (GET, POST, PUT, DELETE)
   * @param path The URL path (e.g., "/connectors" or "/connectors/name/status")
   * @param requestBody The request body (null for GET/DELETE)
   * @return HttpResult containing response code and body
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  private static HttpResult httpRequest(String method, String path, String requestBody)
      throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod(method);

    if (requestBody != null) {
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      try (OutputStream os = conn.getOutputStream()) {
        os.write(requestBody.getBytes());
        os.flush();
      }
    } else {
      conn.setRequestProperty("Accept", "application/json");
    }

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log(
        "%s %s - Response code: %d, Body: %s", method, url, responseCode, responseBody);
    conn.disconnect();

    return new HttpResult(responseCode, responseBody);
  }

  /**
   * Reads the response body from an HTTP connection.
   *
   * @param conn The HTTP connection
   * @return The response body as a string
   */
  private static String readResponse(HttpURLConnection conn) {
    String responseBody = "";

    try {
      int responseCode = conn.getResponseCode();

      // Read response body
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream()))) {
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          response.append(line);
        }
        responseBody = response.toString();
      } catch (Exception e) {
        responseBody = "Failed to read response body: " + e.getMessage();
      }

    } catch (Exception e) {
      responseBody = "Failed to read response: " + e.getMessage();
    }

    return responseBody;
  }

  /**
   * Calculates the exponential backoff delay for a given retry attempt.
   *
   * @param attempt The current retry attempt (0-based)
   * @return The delay in milliseconds, capped at MAX_BACKOFF_MS
   */
  private static long calculateBackoffDelay(int attempt) {
    long delay = INITIAL_BACKOFF_MS * (1L << attempt); // 2^attempt * initial
    return Math.min(delay, MAX_BACKOFF_MS);
  }

  /**
   * Sleeps for the exponential backoff duration for the given retry attempt.
   *
   * @param attempt The current retry attempt (0-based)
   * @throws InterruptedException if the thread is interrupted during sleep
   */
  private static void sleepWithBackoff(int attempt) throws InterruptedException {
    long delay = calculateBackoffDelay(attempt);
    logger.atFine().log("Retry attempt %d, sleeping for %d ms", attempt + 1, delay);
    Thread.sleep(delay);
  }

  /**
   * Extracts the connector name from a connector configuration JSON string.
   *
   * @param connectorConfigJson The connector configuration as a JSON string
   * @return The connector name, or null if not found
   */
  private static String extractConnectorName(String connectorConfigJson) {
    // Simple parsing for "name":"value" pattern
    int nameIndex = connectorConfigJson.indexOf("\"name\"");
    if (nameIndex == -1) {
      return null;
    }
    int colonIndex = connectorConfigJson.indexOf(":", nameIndex);
    if (colonIndex == -1) {
      return null;
    }
    int startQuote = connectorConfigJson.indexOf("\"", colonIndex);
    if (startQuote == -1) {
      return null;
    }
    int endQuote = connectorConfigJson.indexOf("\"", startQuote + 1);
    if (endQuote == -1) {
      return null;
    }
    return connectorConfigJson.substring(startQuote + 1, endQuote);
  }

  /**
   * Registers a connector with the running Kafka Connect cluster. Retries on 5xx errors with
   * exponential backoff, checking if the connector was actually created before each retry.
   *
   * @param connectorConfigJson The connector configuration as a JSON string
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if registration fails or Kafka Connect is not running
   */
  public static void registerConnector(String connectorConfigJson) {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    String connectorName = extractConnectorName(connectorConfigJson);
    if (connectorName == null) {
      throw new IllegalStateException("Connector name should be present");
    }

    int responseCode = 0;
    String responseBody = "";

    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      URL url;
      try {
        url = new URL(connectUrl + "/connectors");
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
      HttpURLConnection conn;
      try {
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
          os.write(connectorConfigJson.getBytes());
          os.flush();
        }

        responseBody = readResponse(conn);
        responseCode = conn.getResponseCode();
      } catch (Exception e) {
        logger.atFinest().log(
            "POST %s - Failed to send query (attempt %d): %s", url, attempt + 1, e.getMessage());
        continue;
      }
      logger.atFinest().log(
          "POST %s - Response code: %d, Body: %s (attempt %d)",
          url, responseCode, responseBody, attempt + 1);
      conn.disconnect();

      // Check if connector was actually created before retrying
      try {
        String status = getConnectorStatus(connectorName);
        if (status != null) {
          logger.atFine().log("Connector %s was confirmed to be present", connectorName);
          return;
        }
      } catch (Exception e) {
        logger.atFine().log("Failed to check connector status during retry: %s", e.getMessage());
      }

      // Retry with backoff if we have more attempts
      if (attempt < MAX_RETRIES) {
        try {
          sleepWithBackoff(attempt);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    throw new RuntimeException(
        "Failed to register connector after "
            + (MAX_RETRIES + 1)
            + " attempts. Last response code: "
            + responseCode
            + ", body: "
            + responseBody);
  }

  /**
   * Registers a connector with the running Kafka Connect cluster using a Properties object
   * containing key-value configuration pairs.
   *
   * @param properties Connector configuration properties
   * @param connectorName The name of the connector to register
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if registration fails or Kafka Connect is not running
   */
  public static void registerConnector(Properties properties, String connectorName) {
    String connectorConfigJson = propertiesToConnectorJson(connectorName, properties);
    registerConnector(connectorConfigJson);
    try {
      KafkaConnectUtils.waitForConnectorRunning(connectorName, 30_000);
    } catch (Exception e) {
      RuntimeException ex = new RuntimeException("Connector failed to reach RUNNING state.", e);
      ex.addSuppressed(e);
      throw ex;
    }
  }

  /**
   * Gets the status of a connector from the running Kafka Connect cluster. Retries on 500 errors
   * with exponential backoff.
   *
   * @param connectorName The name of the connector
   * @return JSON string containing the connector status, or null if connector doesn't exist
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static String getConnectorStatus(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    int responseCode = 0;
    String responseBody = "";

    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      URL url = new URL(connectUrl + "/connectors/" + connectorName + "/status");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");

      responseBody = readResponse(conn);
      responseCode = conn.getResponseCode();
      logger.atFinest().log(
          "GET %s - Response code: %d, Body: %s (attempt %d)",
          url, responseCode, responseBody, attempt + 1);
      conn.disconnect();

      if (responseCode == 404) {
        return null; // Connector doesn't exist
      }

      if (responseCode == 200) {
        return responseBody;
      }

      // Retry on 5xx errors
      if (responseCode / 100 == 5) {
        if (attempt < MAX_RETRIES) {
          sleepWithBackoff(attempt);
          continue;
        }
      }

      // Other errors - don't retry
      throw new RuntimeException(
          "Failed to get connector status. HTTP error code: " + responseCode);
    }

    throw new RuntimeException(
        "Failed to get connector status after "
            + (MAX_RETRIES + 1)
            + " attempts. Last response code: "
            + responseCode
            + ", body: "
            + responseBody);
  }

  /**
   * Pauses a connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to pause
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int pauseConnector(String connectorName) throws Exception {
    HttpResult result = httpRequest("PUT", "/connectors/" + connectorName + "/pause", "");
    return result.code;
  }

  /**
   * Resumes a paused connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to resume
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int resumeConnector(String connectorName) throws Exception {
    HttpResult result = httpRequest("PUT", "/connectors/" + connectorName + "/resume", "");
    return result.code;
  }

  /**
   * Waits for a connector to reach RUNNING state, or throws if it fails or times out.
   *
   * @param connectorName The name of the connector
   * @param timeoutMs Maximum time to wait
   */
  public static void waitForConnectorRunning(String connectorName, long timeoutMs)
      throws Exception {
    long start = System.currentTimeMillis();
    String lastStatus = null;
    while (System.currentTimeMillis() - start < timeoutMs) {
      lastStatus = getConnectorStatus(connectorName);
      if (lastStatus == null) {
        Thread.sleep(1000);
        continue;
      }
      if (lastStatus.contains("\"state\":\"FAILED\"")) {
        throw new IllegalStateException("Connector entered FAILED state: " + lastStatus);
      }
      if (lastStatus.contains("\"state\":\"RUNNING\"")) {
        return;
      }
      Thread.sleep(1000);
    }
    throw new IllegalStateException(
        "Connector did not reach RUNNING state within "
            + timeoutMs
            + " ms. Last status: "
            + lastStatus);
  }

  /**
   * Deletes a connector from the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to delete
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int deleteConnector(String connectorName) throws Exception {
    HttpResult result = httpRequest("DELETE", "/connectors/" + connectorName, null);
    return result.code;
  }

  /**
   * Removes a single connector from the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to delete
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int removeConnector(String connectorName) throws Exception {
    return deleteConnector(connectorName);
  }

  /**
   * Lists all connectors in the running Kafka Connect cluster.
   *
   * @return JSON string containing the list of connector names
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static String getAllConnectors() throws Exception {
    HttpResult result = httpRequest("GET", "/connectors", null);
    if (result.code != 200) {
      throw new RuntimeException("Failed to get connectors. HTTP error code: " + result.code);
    }
    return result.body;
  }

  /**
   * Gets the configuration of a connector from the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector
   * @return JSON string containing the connector configuration, or null if connector doesn't exist
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static String getConnectorConfig(String connectorName) throws Exception {
    HttpResult result = httpRequest("GET", "/connectors/" + connectorName + "/config", null);
    if (result.code == 404) {
      return null; // Connector doesn't exist
    }
    if (result.code != 200) {
      throw new RuntimeException("Failed to get connector config. HTTP error code: " + result.code);
    }
    return result.body;
  }

  /**
   * Updates the configuration of an existing connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to update
   * @param connectorConfigJson The new connector configuration as a JSON string
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int updateConnectorConfig(String connectorName, String connectorConfigJson)
      throws Exception {
    HttpResult result =
        httpRequest("PUT", "/connectors/" + connectorName + "/config", connectorConfigJson);
    return result.code;
  }

  /**
   * Restarts a connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to restart
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int restartConnector(String connectorName) throws Exception {
    HttpResult result = httpRequest("POST", "/connectors/" + connectorName + "/restart", "");
    return result.code;
  }

  /**
   * Removes all connectors from the running Kafka Connect cluster. This method first retrieves the
   * list of all connectors and then deletes each one.
   *
   * @return The number of connectors that were successfully deleted
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int removeAllConnectors() throws Exception {
    // First, get the list of all connectors (this will check if Kafka Connect is running)
    String connectorsJson = getAllConnectors();

    // Parse the JSON array to extract connector names
    // The response is a JSON array like: ["connector1", "connector2", ...]
    String[] connectorNames = parseConnectorNames(connectorsJson);

    int deletedCount = 0;
    for (String connectorName : connectorNames) {
      try {
        int responseCode = deleteConnector(connectorName);
        if (responseCode >= 200 && responseCode < 300) {
          deletedCount++;
        }
      } catch (Exception e) {
        // Log the error but continue with other connectors
        System.err.println("Failed to delete connector: " + connectorName + " - " + e.getMessage());
      }
    }

    return deletedCount;
  }

  /**
   * Parses a JSON array of connector names and returns them as a String array. Input format:
   * ["connector1", "connector2", ...]
   *
   * @param connectorsJson JSON array string containing connector names
   * @return Array of connector names
   */
  private static String[] parseConnectorNames(String connectorsJson) {
    if (connectorsJson == null || connectorsJson.trim().isEmpty()) {
      return new String[0];
    }

    // Remove brackets and whitespace
    String cleaned = connectorsJson.trim();
    if (cleaned.startsWith("[") && cleaned.endsWith("]")) {
      cleaned = cleaned.substring(1, cleaned.length() - 1).trim();
    }

    // If empty array, return empty string array
    if (cleaned.isEmpty()) {
      return new String[0];
    }

    // Split by comma and clean up each name
    String[] parts = cleaned.split(",");
    String[] connectorNames = new String[parts.length];

    for (int i = 0; i < parts.length; i++) {
      String name = parts[i].trim();
      // Remove quotes if present
      if (name.startsWith("\"") && name.endsWith("\"")) {
        name = name.substring(1, name.length() - 1);
      }
      connectorNames[i] = name;
    }

    return connectorNames;
  }

  /**
   * Converts a Properties object and connector name into a JSON string that can be used with the
   * Kafka Connect REST API to register a new connector.
   *
   * @param connectorName The name of the connector
   * @param properties The connector configuration properties
   * @return JSON string suitable for Kafka Connect REST API
   */
  public static String propertiesToConnectorJson(String connectorName, Properties properties) {
    StringBuilder json = new StringBuilder();
    json.append("{");
    json.append("\"name\":\"").append(connectorName).append("\",");
    json.append("\"config\":{");

    boolean first = true;
    for (String key : properties.stringPropertyNames()) {
      if (!first) {
        json.append(",");
      }
      json.append("\"").append(key).append("\":");
      json.append("\"").append(properties.getProperty(key)).append("\"");
      first = false;
    }

    json.append("}");
    json.append("}");
    return json.toString();
  }

  /**
   * Creates a common set of properties for the Scylla connector that can be used in tests. Some
   * required properties like "topic.prefix" and "scylla.table.names" are test specific and should
   * be set by the test case. Uses JSON serialization without schemas for key and value.
   *
   * @return Properties object with common connector settings
   */
  public static @NotNull Properties createCommonConnectorProperties() {
    Properties connectorConfiguration = new Properties();
    connectorConfiguration.put(
        "connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector");
    connectorConfiguration.put("scylla.cluster.ip.addresses", "scylla:9042");
    connectorConfiguration.put("tasks.max", "1");
    // Lower than defaults to speed up tests
    connectorConfiguration.put("scylla.query.time.window.size", "10000");
    connectorConfiguration.put("scylla.confidence.window.size", "5000");
    connectorConfiguration.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    connectorConfiguration.put("key.converter.schemas.enable", "false");
    connectorConfiguration.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    connectorConfiguration.put("value.converter.schemas.enable", "false");
    return connectorConfiguration;
  }

  /**
   * Creates a set of properties for the Scylla connector that uses Avro serialization. This is
   * suitable for tests that require Avro format with Schema Registry support. Created instance
   * requires adding "topic.prefix" and "scylla.table.names" properties.
   *
   * @return Properties object with Avro-specific connector settings
   */
  public static Properties createAvroConnectorProperties() {
    Properties connectorConfiguration = new Properties();
    connectorConfiguration.put(
        "connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector");
    connectorConfiguration.put("scylla.cluster.ip.addresses", "scylla:9042");
    connectorConfiguration.put("tasks.max", "1");
    // Lower than defaults to speed up tests
    connectorConfiguration.put("scylla.query.time.window.size", "10000");
    connectorConfiguration.put("scylla.confidence.window.size", "5000");
    connectorConfiguration.put("key.converter", "io.confluent.connect.avro.AvroConverter");
    connectorConfiguration.put("key.converter.schemas.enable", "true");
    connectorConfiguration.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    connectorConfiguration.put("value.converter.schemas.enable", "true");

    String schemaRegistryUrl = getSchemaRegistryUrlForConnector();
    if (schemaRegistryUrl != null) {
      connectorConfiguration.put("key.converter.schema.registry.url", schemaRegistryUrl);
      connectorConfiguration.put("value.converter.schema.registry.url", schemaRegistryUrl);
    } else {
      throw new IllegalStateException(
          "Schema Registry URL is not set. Cannot create Avro connector properties.");
    }

    return connectorConfiguration;
  }

  /**
   * Gets the Schema Registry URL that will work inside the container network.
   *
   * @return The Schema Registry URL, or null if not running
   */
  public static String getSchemaRegistryUrlForConnector() {
    if (AbstractContainerBaseIT.getSchemaRegistryUrl() == null) {
      return null;
    }
    return "http://schema-registry:" + AbstractContainerBaseIT.SCHEMA_REGISTRY_PORT;
  }

  /** Default primary key placement configuration for all locations. */
  static final String DEFAULT_PK_PLACEMENT =
      "kafka-key,payload-after,payload-before,payload-diff,payload-key,kafka-headers";

  /**
   * Applies advanced CDC configuration to connector properties. Use this for tests that need
   * cdc.include.* configurations (before, after, primary-key.placement).
   *
   * @param props the properties to configure
   * @param connectorName the connector name
   * @param tableName the table name
   */
  private static void applyAdvancedCdcConfig(
      Properties props, String connectorName, String tableName) {
    props.put("topic.prefix", connectorName);
    props.put("scylla.table.names", tableName);
    props.put("name", connectorName);
    props.put("cdc.output.format", "advanced");
    props.put("cdc.include.before", "full");
    props.put("cdc.include.after", "full");
    props.put("cdc.include.primary-key.placement", DEFAULT_PK_PLACEMENT);
  }

  /**
   * Applies legacy CDC configuration to connector properties. Legacy mode does NOT support
   * cdc.include.* configurations - use experimental.preimages.enabled instead for preimage data.
   *
   * @param props the properties to configure
   * @param connectorName the connector name
   * @param tableName the table name
   */
  private static void applyLegacyCdcConfig(
      Properties props, String connectorName, String tableName) {
    props.put("topic.prefix", connectorName);
    props.put("scylla.table.names", tableName);
    props.put("name", connectorName);
    // Legacy format is the default, but we explicitly set it for clarity
    props.put("cdc.output.format", "legacy");
    // Note: cdc.include.* configurations are NOT supported in legacy mode
    // Use experimental.preimages.enabled for preimage data in legacy mode
  }

  /**
   * Registers a connector and subscribes the consumer to its topic.
   *
   * @param consumer the Kafka consumer
   * @param connectorName the connector name
   * @param tableName the table name
   * @param props the connector properties
   */
  private static <K, V> void registerAndSubscribe(
      KafkaConsumer<K, V> consumer, String connectorName, String tableName, Properties props) {
    registerConnector(props, connectorName);
    consumer.subscribe(List.of(connectorName + "." + tableName));
  }

  static KafkaConsumer<GenericRecord, GenericRecord> buildAvroConnector(
      String connectorConfigName, String tableName) {
    KafkaConsumer<GenericRecord, GenericRecord> consumer = KafkaUtils.createAvroConsumer();
    Properties connectorConfiguration = KafkaConnectUtils.createAvroConnectorProperties();
    applyAdvancedCdcConfig(connectorConfiguration, connectorConfigName, tableName);
    registerAndSubscribe(consumer, connectorConfigName, tableName, connectorConfiguration);
    return consumer;
  }

  static KafkaConsumer<String, String> buildScyllaExtractNewRecordStateConnector(
      String connectorConfigName, String tableName) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
    applyAdvancedCdcConfig(connectorConfiguration, connectorConfigName, tableName);
    connectorConfiguration.put("transforms", "extractNewRecordState");
    connectorConfiguration.put(
        "transforms.extractNewRecordState.type",
        "com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState");
    // Keep tombstone records for DELETE events (default is to drop them)
    connectorConfiguration.put("transforms.extractNewRecordState.drop.tombstones", "false");
    registerAndSubscribe(consumer, connectorConfigName, tableName, connectorConfiguration);
    return consumer;
  }

  static KafkaConsumer<String, String> buildPlainConnector(
      String connectorConfigName, String tableName) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
    applyAdvancedCdcConfig(connectorConfiguration, connectorConfigName, tableName);
    registerAndSubscribe(consumer, connectorConfigName, tableName, connectorConfiguration);
    return consumer;
  }

  static KafkaConsumer<String, String> buildLegacyPlainConnector(
      String connectorConfigName, String tableName, boolean preimagesEnabled) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
    applyLegacyCdcConfig(connectorConfiguration, connectorConfigName, tableName);
    // Legacy mode uses experimental.preimages.enabled instead of cdc.include.before/after
    connectorConfiguration.put("experimental.preimages.enabled", String.valueOf(preimagesEnabled));
    registerAndSubscribe(consumer, connectorConfigName, tableName, connectorConfiguration);
    return consumer;
  }

  static KafkaConsumer<String, String> buildLegacyScyllaExtractNewRecordStateConnector(
      String connectorConfigName, String tableName) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
    applyLegacyCdcConfig(connectorConfiguration, connectorConfigName, tableName);
    // Legacy mode uses experimental.preimages.enabled instead of cdc.include.before/after
    connectorConfiguration.put("transforms", "extractNewRecordState");
    connectorConfiguration.put(
        "transforms.extractNewRecordState.type",
        "com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState");
    // Keep tombstone records for DELETE events (default is to drop them)
    connectorConfiguration.put("transforms.extractNewRecordState.drop.tombstones", "false");
    registerAndSubscribe(consumer, connectorConfigName, tableName, connectorConfiguration);
    return consumer;
  }

  static void waitForConsumerAssignment(KafkaConsumer<?, ?> consumer) {
    long startTime = System.currentTimeMillis();
    while (consumer.assignment().isEmpty()
        && System.currentTimeMillis() - startTime < CONSUMER_TIMEOUT) {
      consumer.poll(java.time.Duration.ofSeconds(1));
    }
    if (consumer.assignment().isEmpty()) {
      Assertions.fail(
          "Consumer did not receive partition assignment within " + CONSUMER_TIMEOUT + " ms.");
    }
    consumer.seekToBeginning(consumer.assignment());
  }
}
