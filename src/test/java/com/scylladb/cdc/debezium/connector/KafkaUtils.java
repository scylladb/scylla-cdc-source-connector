package com.scylladb.cdc.debezium.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Utility class for creating Kafka consumers for testing purposes. */
public final class KafkaUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private KafkaUtils() {
    throw new UnsupportedOperationException(
        "KafkaUtils is a utility class and cannot be instantiated");
  }

  /**
   * Creates a KafkaConsumer capable of consuming String messages. Uses String deserializers for
   * both key and value.
   *
   * @return a configured KafkaConsumer for String key/value pairs
   */
  public static KafkaConsumer<String, String> createStringConsumer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", AbstractContainerBaseIT.getKafkaBootstrapServers());
    props.put("group.id", "test-consumer-group-" + System.currentTimeMillis());
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "true");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());

    return new KafkaConsumer<>(props);
  }

  /**
   * Creates a KafkaConsumer capable of consuming Avro messages. Uses Avro deserializers for both
   * key and value with Schema Registry support.
   *
   * @return a configured KafkaConsumer for Avro key/value pairs
   */
  public static KafkaConsumer<GenericRecord, GenericRecord> createAvroConsumer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", AbstractContainerBaseIT.getKafkaBootstrapServers());
    props.put("group.id", "test-avro-consumer-group-" + System.currentTimeMillis());
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "true");
    props.put("key.deserializer", KafkaAvroDeserializer.class.getName());
    props.put("value.deserializer", KafkaAvroDeserializer.class.getName());

    // Add Schema Registry URL if available (for Confluent setup)
    String schemaRegistryUrl = AbstractContainerBaseIT.getSchemaRegistryUrl();
    if (schemaRegistryUrl != null) {
      props.put("schema.registry.url", schemaRegistryUrl);
    }

    return new KafkaConsumer<>(props);
  }

  /**
   * Parses a Debezium-style JSON envelope and extracts a collection of strings from the given field
   * under the {@code after} section. Expects the structure:
   *
   * <pre><code>{ "after": { fieldName: { "value": [ ... ]}}}</code></pre>
   */
  public static <T> List<T> extractListFromAfterField(
      String json, String fieldName, Function<JsonNode, T> mapper) {
    try {
      JsonNode valueNode = extractValueNodeFromAfterField(json, fieldName);
      if (!valueNode.isArray()) {
        throw new IllegalStateException(
            "Expected array at after." + fieldName + ".value in JSON: " + json);
      }

      var result = new ArrayList<T>();
      for (JsonNode element : valueNode) {
        result.add(mapper.apply(element));
      }
      return result;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to parse JSON for field '" + fieldName + "'", e);
    }
  }

  /**
   * Parses a Debezium-style JSON envelope and returns the {@code value} node for a given field
   * under the {@code after} section. Expects the structure:
   *
   * <pre><code>{ "after": { fieldName: { "value": ... }}}</code></pre>
   */
  public static JsonNode extractValueNodeFromAfterField(String json, String fieldName) {
    try {
      JsonNode root = OBJECT_MAPPER.readTree(json);
      JsonNode after = root.get("after");
      if (after == null || after.isNull()) {
        throw new IllegalStateException("Expected non-null 'after' field in JSON: " + json);
      }

      return after.path(fieldName).path("value");
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to parse JSON value node for field '" + fieldName + "'", e);
    }
  }
}
