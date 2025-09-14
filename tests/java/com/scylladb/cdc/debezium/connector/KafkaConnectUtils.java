package com.scylladb.cdc.debezium.connector;

import com.google.common.flogger.FluentLogger;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import org.jetbrains.annotations.NotNull;

/** Utility class for Kafka Connect operations. */
public final class KafkaConnectUtils {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  private KafkaConnectUtils() {
    throw new UnsupportedOperationException("This utility class cannot be instantiated");
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
   * Registers a connector with the running Kafka Connect cluster.
   *
   * @param connectorConfigJson The connector configuration as a JSON string
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if registration fails or Kafka Connect is not running
   */
  public static int registerConnector(String connectorConfigJson) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    try (OutputStream os = conn.getOutputStream()) {
      os.write(connectorConfigJson.getBytes());
      os.flush();
    }

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("POST %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
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
  public static int registerConnector(Properties properties, String connectorName)
      throws Exception {
    String connectorConfigJson = propertiesToConnectorJson(connectorName, properties);
    return registerConnector(connectorConfigJson);
  }

  /**
   * Gets the status of a connector from the running Kafka Connect cluster.
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

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/status");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("GET %s - Response code: %d, Body: %s", url, responseCode, responseBody);

    if (responseCode == 404) {
      conn.disconnect();
      return null; // Connector doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to get connector status. HTTP error code: " + responseCode);
    }

    conn.disconnect();
    return responseBody;
  }

  /**
   * Pauses a connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to pause
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int pauseConnector(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/pause");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("PUT %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
  }

  /**
   * Resumes a paused connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to resume
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int resumeConnector(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/resume");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("PUT %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
  }

  /**
   * Deletes a connector from the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to delete
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int deleteConnector(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("DELETE");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log(
        "DELETE %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
  }

  /**
   * Lists all connectors in the running Kafka Connect cluster.
   *
   * @return JSON string containing the list of connector names
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static String getAllConnectors() throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("GET %s - Response code: %d, Body: %s", url, responseCode, responseBody);

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to get connectors. HTTP error code: " + responseCode);
    }

    conn.disconnect();
    return responseBody;
  }

  /**
   * Gets the configuration of a connector from the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector
   * @return JSON string containing the connector configuration, or null if connector doesn't exist
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static String getConnectorConfig(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("GET %s - Response code: %d, Body: %s", url, responseCode, responseBody);

    if (responseCode == 404) {
      conn.disconnect();
      return null; // Connector doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to get connector config. HTTP error code: " + responseCode);
    }

    conn.disconnect();
    return responseBody;
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
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("PUT");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    try (OutputStream os = conn.getOutputStream()) {
      os.write(connectorConfigJson.getBytes());
      os.flush();
    }

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("PUT %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
  }

  /**
   * Restarts a connector in the running Kafka Connect cluster.
   *
   * @param connectorName The name of the connector to restart
   * @return The HTTP response code from the Connect REST API
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int restartConnector(String connectorName) throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    URL url = new URL(connectUrl + "/connectors/" + connectorName + "/restart");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");

    String responseBody = readResponse(conn);
    int responseCode = conn.getResponseCode();
    logger.atFinest().log("POST %s - Response code: %d, Body: %s", url, responseCode, responseBody);
    conn.disconnect();
    return responseCode;
  }

  /**
   * Removes all connectors from the running Kafka Connect cluster. This method first retrieves the
   * list of all connectors and then deletes each one.
   *
   * @return The number of connectors that were successfully deleted
   * @throws Exception if the request fails or Kafka Connect is not running
   */
  public static int removeAllConnectors() throws Exception {
    String connectUrl = AbstractContainerBaseIT.getKafkaConnectUrl();
    if (connectUrl == null) {
      throw new IllegalStateException("Kafka Connect is not running");
    }

    // First, get the list of all connectors
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
}
