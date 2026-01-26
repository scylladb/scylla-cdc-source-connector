package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class StartConnectorIT extends AbstractContainerBaseIT {

  @AfterEach
  public void cleanUp(TestInfo testInfo) {
    try {
      KafkaConnectUtils.removeConnector(connectorName(testInfo));
    } catch (Exception e) {
      throw new RuntimeException("Failed to remove connector: " + connectorName(testInfo), e);
    }
  }

  @Test
  public void canRegisterScyllaConnector(TestInfo testInfo) {
    String connectorName = connectorName(testInfo);

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Session session = cluster.connect();
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS "
              + "connectortest"
              + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
      // Create a table with cdc enabled
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + "connectortest.test_table"
              + " (id int PRIMARY KEY, name text) WITH cdc = {'enabled':true, 'preimage':true, 'postimage':true}");
      session.execute("INSERT INTO connectortest.test_table (id, name) VALUES (1, 'test_text');");
      session.close();
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", connectorName);
      connectorConfiguration.put("scylla.table.names", "connectortest.test_table");
      connectorConfiguration.put("name", connectorName);
      connectorConfiguration.put("cdc.include.before", "full");
      connectorConfiguration.put("cdc.include.after", "full");
      connectorConfiguration.put(
          "cdc.include.primary-key.placement",
          "kafka-key,payload-after,payload-before,payload-diff,payload-key,kafka-headers");
      KafkaConnectUtils.registerConnector(connectorConfiguration, connectorName);
      consumer.subscribe(List.of(connectorName + ".connectortest.test_table"));
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
                // With cdc.include.primary-key.placement=...payload-after..., the PK should be in
                // the after field
                assert record.value().contains("\"id\":1")
                    && record.value().contains("\"name\":\"test_text\"");
              });
          break;
        }
      }
      consumer.unsubscribe();
      assert messageConsumed;
    }
  }
}
