package com.scylladb.cdc.debezium.connector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/** Utility class for Schema Registry operations. */
public final class SchemaRegistryUtils {

  private SchemaRegistryUtils() {
    throw new UnsupportedOperationException("This utility class cannot be instantiated");
  }

  /**
   * Retrieves all subjects registered in the running Schema Registry.
   *
   * @return JSON string containing the list of all subjects
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getAllSubjects() throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to get subjects. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Retrieves all available schema versions for a given subject from the running Schema Registry.
   *
   * @param subject The subject name (typically follows the pattern: topicName-key or
   *     topicName-value)
   * @return JSON string containing the list of version numbers, or null if subject doesn't exist
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getSchemaVersions(String subject) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects/" + subject + "/versions");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to get schema versions. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Retrieves the schema details for a specific version of a subject from the running Schema
   * Registry.
   *
   * @param subject The subject name (typically follows the pattern: topicName-key or
   *     topicName-value)
   * @param version The version number, or "latest" for the most recent version
   * @return JSON string containing the schema details (id, version, schema), or null if not found
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getSchemaByVersion(String subject, String version) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects/" + subject + "/versions/" + version);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject or version doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to get schema. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Retrieves the latest schema for a given subject from the running Schema Registry.
   *
   * @param subject The subject name (typically follows the pattern: topicName-key or
   *     topicName-value)
   * @return JSON string containing the latest schema details, or null if subject doesn't exist
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getLatestSchema(String subject) throws Exception {
    return getSchemaByVersion(subject, "latest");
  }

  /**
   * Retrieves schema by its global ID from the running Schema Registry.
   *
   * @param schemaId The global schema ID
   * @return JSON string containing the schema details, or null if not found
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getSchemaById(int schemaId) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/schemas/ids/" + schemaId);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Schema ID doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to get schema by ID. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Registers a new schema for a subject in the Schema Registry.
   *
   * @param subject The subject name
   * @param schemaJson The schema definition as JSON string
   * @return JSON string containing the schema ID, or null if registration failed
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String registerSchema(String subject, String schemaJson) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects/" + subject + "/versions");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
    conn.setDoOutput(true);

    // Wrap the schema in the expected format
    String requestBody = "{\"schema\":\"" + schemaJson.replace("\"", "\\\"") + "\"}";

    try (OutputStream os = conn.getOutputStream()) {
      os.write(requestBody.getBytes());
      os.flush();
    }

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to register schema. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Checks if a schema is compatible with the latest version of a subject.
   *
   * @param subject The subject name
   * @param schemaJson The schema definition as JSON string to check compatibility
   * @return JSON string containing compatibility result
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String checkSchemaCompatibility(String subject, String schemaJson)
      throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url =
        new URL(schemaRegistryUrl + "/compatibility/subjects/" + subject + "/versions/latest");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/vnd.schemaregistry.v1+json");
    conn.setDoOutput(true);

    // Wrap the schema in the expected format
    String requestBody = "{\"schema\":\"" + schemaJson.replace("\"", "\\\"") + "\"}";

    try (OutputStream os = conn.getOutputStream()) {
      os.write(requestBody.getBytes());
      os.flush();
    }

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to check schema compatibility. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Deletes a specific version of a subject from the Schema Registry.
   *
   * @param subject The subject name
   * @param version The version number to delete, or "latest" for the most recent version
   * @return The version number that was deleted
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String deleteSchemaVersion(String subject, String version) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects/" + subject + "/versions/" + version);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("DELETE");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject or version doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to delete schema version. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Deletes an entire subject and all its versions from the Schema Registry.
   *
   * @param subject The subject name to delete
   * @return JSON array of version numbers that were deleted
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String deleteSubject(String subject) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/subjects/" + subject);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("DELETE");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject doesn't exist
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException("Failed to delete subject. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Gets the global compatibility level configuration from the Schema Registry.
   *
   * @return JSON string containing the compatibility configuration
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getGlobalCompatibility() throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/config");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to get global compatibility. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }

  /**
   * Gets the compatibility level configuration for a specific subject.
   *
   * @param subject The subject name
   * @return JSON string containing the subject's compatibility configuration, or null if not set
   * @throws Exception if the request fails or Schema Registry is not running
   */
  public static String getSubjectCompatibility(String subject) throws Exception {
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl == null) {
      throw new IllegalStateException("Schema Registry container is not running");
    }

    URL url = new URL(schemaRegistryUrl + "/config/" + subject);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");

    int responseCode = conn.getResponseCode();
    if (responseCode == 404) {
      conn.disconnect();
      return null; // Subject doesn't have specific compatibility setting
    }

    if (responseCode != 200) {
      conn.disconnect();
      throw new RuntimeException(
          "Failed to get subject compatibility. HTTP error code: " + responseCode);
    }

    StringBuilder response = new StringBuilder();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
    }
    conn.disconnect();
    return response.toString();
  }
}
