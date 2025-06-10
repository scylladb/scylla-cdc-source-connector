package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.List;

public class ScyllaTypesIT extends BaseConnectorIT {

  @Test
  public void canReplicateAllPrimitiveTypes() throws UnknownHostException {
    // Does not include counter columns, as they are not allowed to be mixed with non-counter columns in the same table.
    try (Cluster cluster =
             Cluster
                 .builder()
                 .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                 .withPort(scyllaDBContainer.getMappedPort(9042))
                 .build();
         KafkaConsumer<String, String> consumer =
             getConsumer(kafkaContainer)) {
      Session session = cluster.connect();
      session.execute("CREATE KEYSPACE IF NOT EXISTS primitive_types_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
      session.execute("CREATE TABLE IF NOT EXISTS primitive_types_ks.tab ("
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
      session.execute("INSERT INTO primitive_types_ks.tab (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col) VALUES ("
          + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'some text', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999)"
          + ";");
      session.close();
      ConnectorConfiguration connector =
          ConnectorConfiguration
              .create()
              .with("connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector")
              .with("scylla.cluster.ip.addresses", "scylla:9042")
              .with("topic.prefix", "canReplicateAllPrimitiveTypes")
              .with("scylla.table.names", "primitive_types_ks.tab")
              .with("tasks.max", "1")
              .with("name", "ScyllaAllTypesConnector")
              .with("scylla.query.time.window.size", "10000")
              .with("scylla.confidence.window.size", "5000");
      debeziumContainer.registerConnector("ScyllaAllTypesConnector", connector);
      consumer.subscribe(List.of("canReplicateAllPrimitiveTypes.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(record -> {
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
            // 45296789 part of the value is the number of milliseconds since midnight that corresponds to '12:34:56.789'
            assert value.contains("\"time_col\":{" + "\"value\":45296789000000}");
            // 1718022896789 is unix timestamp in milliseconds for '2024-06-10T12:34:56.789Z'
            assert value.contains("\"timestamp_col\":{" + "\"value\":1718022896789}");
            assert value.contains("\"timeuuid_col\":{" + "\"value\":\"81d4a030-4632-11f0-9484-409dd8f36eba\"");
            assert value.contains("\"tinyint_col\":{" + "\"value\":5}");
            assert value.contains("\"uuid_col\":{" + "\"value\":\"453662fa-db4b-4938-9033-d8523c0a371c\"}");
            assert value.contains("\"varchar_col\":{" + "\"value\":\"varchar text\"}");
            assert value.contains("\"varint_col\":{" + "\"value\":\"999999999\"}");
          });
          break;
        }
      }
      debeziumContainer.deleteAllConnectors();
      consumer.unsubscribe();
      assert messageConsumed;
    }
  }
}


