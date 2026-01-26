package com.scylladb.cdc.debezium.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for JSON parsing operations in tests.
 *
 * <p>Provides methods to extract values from JSON strings using Jackson for reliable parsing.
 */
public final class JsonTestUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private JsonTestUtils() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  /**
   * Extracts the "id" field value from the root level of a JSON string.
   *
   * @param json the JSON string to parse
   * @return the id value, or -1 if not found or parsing fails
   */
  public static int extractIdFromJson(String json) {
    if (json == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode idNode = root.get("id");
      if (idNode != null && idNode.isNumber()) {
        return idNode.asInt();
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  /**
   * Extracts the "id" field value from the "key" object in a JSON string.
   *
   * <p>Expected format: {"key": {"id": N}, ...}
   *
   * @param json the JSON string to parse
   * @return the id value, or -1 if not found or parsing fails
   */
  public static int extractIdFromKeyField(String json) {
    if (json == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode keyNode = root.get("key");
      if (keyNode != null && keyNode.isObject()) {
        JsonNode idNode = keyNode.get("id");
        if (idNode != null && idNode.isNumber()) {
          return idNode.asInt();
        }
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  /**
   * Extracts the "id" field value from the "after" object in a JSON string.
   *
   * <p>Expected format: {"after": {"id": N}, ...}
   *
   * @param json the JSON string to parse
   * @return the id value, or -1 if not found or parsing fails
   */
  public static int extractIdFromAfter(String json) {
    if (json == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode afterNode = root.get("after");
      if (afterNode != null && afterNode.isObject()) {
        JsonNode idNode = afterNode.get("id");
        if (idNode != null && idNode.isNumber()) {
          return idNode.asInt();
        }
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  /**
   * Extracts the "id" field value from the "before" object in a JSON string.
   *
   * <p>Expected format: {"before": {"id": N}, ...}
   *
   * @param json the JSON string to parse
   * @return the id value, or -1 if not found or parsing fails
   */
  public static int extractIdFromBefore(String json) {
    if (json == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode beforeNode = root.get("before");
      if (beforeNode != null && beforeNode.isObject()) {
        JsonNode idNode = beforeNode.get("id");
        if (idNode != null && idNode.isNumber()) {
          return idNode.asInt();
        }
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  /**
   * Extracts the "name" field value from the "after" object in a JSON string.
   *
   * <p>Expected format: {"after": {"name": "value"}, ...}
   *
   * @param json the JSON string to parse
   * @return the name value, or null if not found or parsing fails
   */
  public static String extractNameFromAfter(String json) {
    if (json == null) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode afterNode = root.get("after");
      if (afterNode != null && afterNode.isObject()) {
        JsonNode nameNode = afterNode.get("name");
        if (nameNode != null && nameNode.isTextual()) {
          return nameNode.asText();
        }
      }
    } catch (Exception e) {
      // Fall through to return null
    }
    return null;
  }

  /**
   * Extracts the "name" field value from the "before" object in a JSON string.
   *
   * <p>Expected format: {"before": {"name": "value"}, ...}
   *
   * @param json the JSON string to parse
   * @return the name value, or null if not found or parsing fails
   */
  public static String extractNameFromBefore(String json) {
    if (json == null) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode beforeNode = root.get("before");
      if (beforeNode != null && beforeNode.isObject()) {
        JsonNode nameNode = beforeNode.get("name");
        if (nameNode != null && nameNode.isTextual()) {
          return nameNode.asText();
        }
      }
    } catch (Exception e) {
      // Fall through to return null
    }
    return null;
  }

  /**
   * Extracts the primary key from the name field value using naming convention.
   *
   * <p>Parses names like "test_123", "delete_456" to extract the numeric suffix as the PK. Searches
   * in "after" first, then "before".
   *
   * @param json the JSON string to parse
   * @return the extracted PK, or -1 if not found or parsing fails
   */
  public static int extractPkFromNameField(String json) {
    String name = extractNameFromAfter(json);
    if (name == null) {
      name = extractNameFromBefore(json);
    }
    if (name == null) {
      return -1;
    }
    int underscoreIndex = name.lastIndexOf('_');
    if (underscoreIndex == -1) {
      return -1;
    }
    try {
      return Integer.parseInt(name.substring(underscoreIndex + 1));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  // ===================== Generic Field Extraction Methods =====================

  /**
   * Extracts an integer field value from the root level of a JSON string.
   *
   * @param json the JSON string to parse
   * @param fieldName the name of the field to extract
   * @return the field value, or -1 if not found or parsing fails
   */
  public static int extractIntFromJson(String json, String fieldName) {
    if (json == null || fieldName == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode fieldNode = root.get(fieldName);
      if (fieldNode != null && fieldNode.isNumber()) {
        return fieldNode.asInt();
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  /**
   * Extracts an integer field value from the "key" object in a JSON string.
   *
   * <p>Expected format: {"key": {"fieldName": N}, ...}
   *
   * @param json the JSON string to parse
   * @param fieldName the name of the field to extract
   * @return the field value, or -1 if not found or parsing fails
   */
  public static int extractIntFromKeyField(String json, String fieldName) {
    if (json == null || fieldName == null) {
      return -1;
    }
    try {
      JsonNode root = MAPPER.readTree(json);
      JsonNode keyNode = root.get("key");
      if (keyNode != null && keyNode.isObject()) {
        JsonNode fieldNode = keyNode.get(fieldName);
        if (fieldNode != null && fieldNode.isNumber()) {
          return fieldNode.asInt();
        }
      }
    } catch (Exception e) {
      // Fall through to return -1
    }
    return -1;
  }

  // ===================== Composite Key Methods (pk1) =====================

  /**
   * Extracts the "pk1" field value from the root level of a JSON string.
   *
   * @param json the JSON string to parse
   * @return the pk1 value, or -1 if not found or parsing fails
   */
  public static int extractPk1FromJson(String json) {
    return extractIntFromJson(json, "pk1");
  }

  /**
   * Extracts the "pk1" field value from the "key" object in a JSON string.
   *
   * <p>Expected format: {"key": {"pk1": N, ...}, ...}
   *
   * @param json the JSON string to parse
   * @return the pk1 value, or -1 if not found or parsing fails
   */
  public static int extractPk1FromKeyField(String json) {
    int pk1 = extractIntFromKeyField(json, "pk1");
    if (pk1 != -1) {
      return pk1;
    }
    // Fallback to root level for backwards compatibility
    return extractPk1FromJson(json);
  }
}
