package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class StartConnectorIT {

  private static Network network = Network.newNetwork();

  private static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.12"))
      .withNetwork(network);

  public static ScyllaDBContainer scyllaDBContainer = new ScyllaDBContainer("scylladb/scylla:6.2")
      .withNetwork(network)
      .withNetworkAliases("scylla")
      .withExposedPorts(9042, 19042);

  public static DebeziumContainer debeziumContainer =
      new DebeziumContainer("quay.io/debezium/connect:2.6.2.Final")
          // Requires connector to be built first
          .withFileSystemBind("target/components/packages/", "/kafka/connect/plugins/")
          .withNetwork(network)
          .withKafka(kafkaContainer)
          .dependsOn(kafkaContainer);

  @BeforeClass
  public static void startContainers() {
    Startables.deepStart(Stream.of(
            kafkaContainer, scyllaDBContainer, debeziumContainer))
        .join();
  }

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

  private KafkaConsumer<String, String> getConsumer(
      KafkaContainer kafkaContainer) {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaContainer.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG,
            "tc-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"),
        new StringDeserializer(),
        new StringDeserializer());
  }
}
