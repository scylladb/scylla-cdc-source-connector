package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StartConnectorIT extends BaseConnectorIT {

  @Test
  public void canRegisterScyllaConnector() {
    try (Cluster cluster =
             Cluster
                 .builder()
                 .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                 .withPort(scyllaDBContainer.getMappedPort(9042))
                 .build();
         KafkaConsumer<String, String> consumer =
             getConsumer(kafkaContainer)) {
      Session session = cluster.connect();
      session.execute("CREATE KEYSPACE IF NOT EXISTS connectortest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
      // Create a table with cdc enabled
      session.execute("CREATE TABLE IF NOT EXISTS connectortest.test_table (id int PRIMARY KEY, name text) WITH cdc = {'enabled':true}");
      session.execute("INSERT INTO connectortest.test_table (id, name) VALUES (1, 'test_text');");
      session.close();
      ConnectorConfiguration connector =
          ConnectorConfiguration
              .create()
              .with("connector.class", "com.scylladb.cdc.debezium.connector.ScyllaConnector")
              .with("scylla.cluster.ip.addresses", "scylla:9042")
              .with("topic.prefix", "namespace")
              .with("scylla.table.names", "connectortest.test_table")
              .with("tasks.max", "1")
              .with("name", "ScyllaTestConnector");
      debeziumContainer.registerConnector("ScyllaTestConnector", connector);
      consumer.subscribe(List.of("namespace.connectortest.test_table"));
      // Wait for at most 65 seconds for the connector to start and generate the message
      // corresponding to the inserted row
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(record -> {
            assert record.value().contains("{\"id\":1,\"name\":{\"value\":\"test_text\"}}");
          });
          break;
        }
      }
      consumer.unsubscribe();
      assert messageConsumed;
    }
  }
}
