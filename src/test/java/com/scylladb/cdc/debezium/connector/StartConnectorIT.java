package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StartConnectorIT extends AbstractContainerBaseIT {

  private static final String SCYLLA_TEST_CONNECTOR = "ScyllaTestConnector";

  @AfterEach
  public void removeAllConnectors() {
    try {
      KafkaConnectUtils.removeAllConnectors();
    } catch (Exception e) {
      throw new RuntimeException("Failed to clean up connectors after the test.", e);
    }
  }

  @Test
  public void canRegisterScyllaConnector() {
    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Session session = cluster.connect();
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS connectortest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
      // Create a table with cdc enabled
      session.execute(
          "CREATE TABLE IF NOT EXISTS connectortest.test_table (id int PRIMARY KEY, name text) WITH cdc = {'enabled':true}");
      session.execute("INSERT INTO connectortest.test_table (id, name) VALUES (1, 'test_text');");
      session.close();
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "namespace");
      connectorConfiguration.put("scylla.table.names", "connectortest.test_table");
      connectorConfiguration.put("name", SCYLLA_TEST_CONNECTOR);
      try {
        int responseCode =
            KafkaConnectUtils.registerConnector(connectorConfiguration, SCYLLA_TEST_CONNECTOR);
        if (responseCode == 500) {
          String status = KafkaConnectUtils.getConnectorStatus(SCYLLA_TEST_CONNECTOR);
          if (status == null) {
            Assertions.fail(
                "Received 500 error on connector registration and connector is not registered.");
          }
        } else if (responseCode / 100 != 2) {
          Assertions.fail(
              "Received non-success response code on connector registration: " + responseCode);
        }
      } catch (Exception e) {
        Assertions.fail("Could not register connector.", e);
      }
      consumer.subscribe(List.of("namespace.connectortest.test_table"));
      // Wait for at most 65 seconds for the connector to start and generate the message
      // corresponding to the inserted row
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
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
