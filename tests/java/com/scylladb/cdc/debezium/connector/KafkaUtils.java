package com.scylladb.cdc.debezium.connector;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Utility class for creating Kafka consumers for testing purposes. */
public final class KafkaUtils {

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
}
