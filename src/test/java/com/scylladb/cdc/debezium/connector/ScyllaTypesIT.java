package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class ScyllaTypesIT extends AbstractContainerBaseIT {

  private static final String SCYLLA_ALL_TYPES_CONNECTOR = "ScyllaAllTypesConnector";

  @BeforeAll
  public static void setupTable() {
    // Does not include counter columns, as they are not allowed to be mixed with non-counter
    // columns in the same table.
    try (Cluster cluster =
        Cluster.builder()
            .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
            .withPort(scyllaDBContainer.getMappedPort(9042))
            .build()) {
      Session session = cluster.connect();
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS primitive_types_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
      session.execute(
          "CREATE TABLE IF NOT EXISTS primitive_types_ks.tab ("
              + "id int PRIMARY KEY,"
              + "ascii_col ascii,"
              + "bigint_col bigint,"
              + "blob_col blob,"
              + "boolean_col boolean,"
              + "date_col date,"
              + "decimal_col decimal,"
              + "double_col double,"
              + "duration_col duration,"
              + "float_col float,"
              + "inet_col inet,"
              + "int_col int,"
              + "smallint_col smallint,"
              + "text_col text,"
              + "time_col time,"
              + "timestamp_col timestamp,"
              + "timeuuid_col timeuuid,"
              + "tinyint_col tinyint,"
              + "uuid_col uuid,"
              + "varchar_col varchar,"
              + "varint_col varint"
              + ") WITH cdc = {'enabled':true}");
      session.execute(
          "INSERT INTO primitive_types_ks.tab (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col) VALUES ("
              + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'some text', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999)"
              + ";");
      session.close();
    }
  }

  @AfterEach
  public void cleanUp() {
    try {
      KafkaConnectUtils.removeAllConnectors();
    } catch (Exception e) {
      throw new RuntimeException("Failed to remove the connectors.", e);
    }
  }

  @Test
  public void canReplicateAllPrimitiveTypes() throws UnknownHostException {
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateAllPrimitiveTypes");
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", SCYLLA_ALL_TYPES_CONNECTOR);
      int returnCode = -1;
      try {
        returnCode =
            KafkaConnectUtils.registerConnector(connectorConfiguration, SCYLLA_ALL_TYPES_CONNECTOR);
        if (returnCode == 500) {
          String status = KafkaConnectUtils.getConnectorStatus(SCYLLA_ALL_TYPES_CONNECTOR);
          if (status == null) {
            Assertions.fail(
                "Received 500 error on connector registration and connector is not registered.");
          }
        } else if (returnCode / 100 != 2) {
          Assertions.fail(
              "Received non-success response code on connector registration: " + returnCode);
        }
      } catch (Exception e) {
        Assertions.fail("Failed to register connector.", e);
      }
      consumer.subscribe(List.of("canReplicateAllPrimitiveTypes.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                String value = record.value();
                assert value.contains("\"id\":1");
                assert value.contains("\"ascii_col\":{" + "\"value\":\"ascii\"}");
                assert value.contains("\"bigint_col\":{" + "\"value\":1234567890123}");
                assert value.contains("\"blob_col\":{" + "\"value\":\"yv66vg==\"}");
                assert value.contains("\"boolean_col\":{" + "\"value\":true}");
                // This is number of days since unix epoch that should correspond to '2024-06-10'
                assert value.contains("\"date_col\":{" + "\"value\":19884}");
                assert value.contains("\"decimal_col\":{" + "\"value\":\"12345.67\"}");
                assert value.contains("\"double_col\":{" + "\"value\":3.14159}");
                assert value.contains("\"duration_col\":{" + "\"value\":\"1d12h30m\"");
                assert value.contains("\"float_col\":{" + "\"value\":2.71828}");
                assert value.contains("\"inet_col\":{" + "\"value\":\"127.0.0.1\"}");
                assert value.contains("\"int_col\":{" + "\"value\":42}");
                assert value.contains("\"smallint_col\":{" + "\"value\":7}");
                assert value.contains("\"text_col\":{" + "\"value\":\"some text\"}");
                // Shows up as 45296789000000.
                // 45296789 part of the value is the number of milliseconds since midnight that
                // corresponds to '12:34:56.789'
                assert value.contains("\"time_col\":{" + "\"value\":45296789000000}");
                // 1718022896789 is unix timestamp in milliseconds for '2024-06-10T12:34:56.789Z'
                assert value.contains("\"timestamp_col\":{" + "\"value\":1718022896789}");
                assert value.contains(
                    "\"timeuuid_col\":{" + "\"value\":\"81d4a030-4632-11f0-9484-409dd8f36eba\"");
                assert value.contains("\"tinyint_col\":{" + "\"value\":5}");
                assert value.contains(
                    "\"uuid_col\":{" + "\"value\":\"453662fa-db4b-4938-9033-d8523c0a371c\"}");
                assert value.contains("\"varchar_col\":{" + "\"value\":\"varchar text\"}");
                assert value.contains("\"varint_col\":{" + "\"value\":\"999999999\"}");
              });
          break;
        }
      }
      consumer.unsubscribe();
      Assertions.assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");
    }
  }

  @Test
  public void canExtractNewRecordState() {
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canExtractNewRecordState");
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", "canExtractNewRecordState");
      connectorConfiguration.put("transforms", "extractNewRecordState");
      connectorConfiguration.put(
          "transforms.extractNewRecordState.type",
          "com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState");
      try {
        KafkaConnectUtils.registerConnector(connectorConfiguration, "canExtractNewRecordState");
      } catch (Exception e) {
        throw new RuntimeException("Failed to register connector.", e);
      }
      consumer.subscribe(List.of("canExtractNewRecordState.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                String value = record.value();
                assert value.contains("{\"ascii_col\":\"ascii\"");
                assert value.contains("\"bigint_col\":1234567890123");
                assert value.contains("\"blob_col\":\"yv66vg==\"");
                assert value.contains("\"boolean_col\":true");
                assert value.contains("\"date_col\":19884");
                assert value.contains("\"decimal_col\":\"12345.67\"");
                assert value.contains("\"double_col\":3.14159");
                assert value.contains("\"duration_col\":\"1d12h30m\"");
                assert value.contains("\"float_col\":2.71828");
                assert value.contains("\"id\":1");
                assert value.contains("\"inet_col\":\"127.0.0.1\"");
                assert value.contains("\"int_col\":42");
                assert value.contains("\"smallint_col\":7");
                assert value.contains("\"text_col\":\"some text\"");
                assert value.contains("\"time_col\":45296789000000");
                assert value.contains("\"timestamp_col\":1718022896789");
                assert value.contains("\"timeuuid_col\":\"81d4a030-4632-11f0-9484-409dd8f36eba\"");
                assert value.contains("\"tinyint_col\":5");
                assert value.contains("\"uuid_col\":\"453662fa-db4b-4938-9033-d8523c0a371c\"");
                assert value.contains("\"varchar_col\":\"varchar text\"");
                assert value.contains("\"varint_col\":\"999999999\"}");
              });
          break;
        }
      }
      consumer.unsubscribe();
      Assertions.assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");
    }
  }

  @Test
  @EnabledIf("isConfluentKafkaProvider")
  public void canReplicateAllPrimitiveTypesWithAvro() {
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "AvroConverter is not available in cp-kafka image.");
    try (KafkaConsumer<GenericRecord, GenericRecord> consumer = KafkaUtils.createAvroConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createAvroConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateAllPrimitiveTypesWithAvro");
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", "ScyllaAllTypesAvroConnector");
      int returnCode = -1;
      try {
        returnCode =
            KafkaConnectUtils.registerConnector(
                connectorConfiguration, "ScyllaAllTypesAvroConnector");
      } catch (Exception e) {
        throw new RuntimeException("Failed to register connector.", e);
      }
      Assertions.assertEquals(201, returnCode, "Connector registration failed");
      consumer.subscribe(List.of("canReplicateAllPrimitiveTypesWithAvro.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                GenericRecord value = record.value();
                GenericRecord after = (GenericRecord) value.get("after");

                // Verify all primitive type fields are present and have expected values
                Assertions.assertEquals("1", extractValue(after.get("id")).toString());
                Assertions.assertEquals("ascii", extractValue(after.get("ascii_col")).toString());
                Assertions.assertEquals(
                    "1234567890123", extractValue(after.get("bigint_col")).toString());
                Assertions.assertNotNull(extractValue(after.get("blob_col"))); // blob as bytes
                Assertions.assertEquals("true", extractValue(after.get("boolean_col")).toString());
                // This is number of days since unix epoch that should correspond to '2024-06-10'
                Assertions.assertEquals("19884", extractValue(after.get("date_col")).toString());
                Assertions.assertEquals(
                    "12345.67", extractValue(after.get("decimal_col")).toString());
                Assertions.assertEquals(
                    "3.14159", extractValue(after.get("double_col")).toString());
                Assertions.assertEquals(
                    "1d12h30m", extractValue(after.get("duration_col")).toString());
                Assertions.assertEquals("2.71828", extractValue(after.get("float_col")).toString());
                Assertions.assertEquals(
                    "127.0.0.1", extractValue(after.get("inet_col")).toString());
                Assertions.assertEquals("42", extractValue(after.get("int_col")).toString());
                Assertions.assertEquals("7", extractValue(after.get("smallint_col")).toString());
                Assertions.assertEquals(
                    "some text", extractValue(after.get("text_col")).toString());
                // Shows up as 45296789000000.
                // 45296789 part of the value is the number of milliseconds since midnight that
                // corresponds to '12:34:56.789'
                Assertions.assertEquals(
                    "45296789000000", extractValue(after.get("time_col")).toString());
                // 1718022896789 is unix timestamp in milliseconds for '2024-06-10T12:34:56.789Z'
                Assertions.assertEquals(
                    "1718022896789", extractValue(after.get("timestamp_col")).toString());
                Assertions.assertEquals(
                    "81d4a030-4632-11f0-9484-409dd8f36eba",
                    extractValue(after.get("timeuuid_col")).toString());
                Assertions.assertEquals("5", extractValue(after.get("tinyint_col")).toString());
                Assertions.assertEquals(
                    "453662fa-db4b-4938-9033-d8523c0a371c",
                    extractValue(after.get("uuid_col")).toString());
                Assertions.assertEquals(
                    "varchar text", extractValue(after.get("varchar_col")).toString());
                Assertions.assertEquals(
                    "999999999", extractValue(after.get("varint_col")).toString());
              });
          break;
        }
      }
      consumer.unsubscribe();
      Assertions.assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");

      // The schema registry check is kind of overzealous. The correctness of that part is
      // mostly the responsibility of the AvroConverter rather than the connector.

      // Verify that schema registry has exactly 1 schema registered for both key and value subjects
      String topicName = "canReplicateAllPrimitiveTypesWithAvro.primitive_types_ks.tab";
      String keySubject = topicName + "-key";
      String valueSubject = topicName + "-value";

      try {
        String keyVersions = SchemaRegistryUtils.getSchemaVersions(keySubject);
        String valueVersions = SchemaRegistryUtils.getSchemaVersions(valueSubject);

        Assertions.assertNotNull(keyVersions, "Key subject should exist in schema registry");
        Assertions.assertNotNull(valueVersions, "Value subject should exist in schema registry");

        Assertions.assertEquals(
            "[1]", keyVersions, "Key subject should have exactly 1 schema version");
        Assertions.assertEquals(
            "[1]", valueVersions, "Key subject should have exactly 1 schema version");

      } catch (Exception e) {
        Assertions.fail("Failed to verify schema registry: " + e.getMessage());
      }
    }
  }

  /** Condition method for enabling tests that require Confluent Kafka (schema registry). */
  static boolean isConfluentKafkaProvider() {
    return KAFKA_PROVIDER == KafkaProvider.CONFLUENT;
  }

  // Helper method to extract value from Avro types
  private static Object extractValue(Object obj) {
    if (obj instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) obj;
      return record.get("value");
    }
    return obj;
  }
}
