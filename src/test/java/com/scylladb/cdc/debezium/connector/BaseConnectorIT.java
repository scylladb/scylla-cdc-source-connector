package com.scylladb.cdc.debezium.connector;

import io.debezium.testing.testcontainers.DebeziumContainer;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.scylladb.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class BaseConnectorIT {
  protected static final Network network = Network.newNetwork();

  protected static final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.12"))
          .withNetwork(network);

  protected static final ScyllaDBContainer scyllaDBContainer =
      new ScyllaDBContainer("scylladb/scylla:6.2")
          .withNetwork(network)
          .withNetworkAliases("scylla")
          .withExposedPorts(9042, 19042);

  protected static final DebeziumContainer debeziumContainer =
      new DebeziumContainer("quay.io/debezium/connect:2.6.2.Final")
          .withFileSystemBind("target/components/packages/", "/kafka/connect/plugins/")
          .withNetwork(network)
          .withKafka(kafkaContainer)
          .dependsOn(kafkaContainer);

  static {
    Startables.deepStart(Stream.of(kafkaContainer, scyllaDBContainer, debeziumContainer)).join();
  }

  protected static KafkaConsumer<String, String> getConsumer(KafkaContainer kafkaContainer) {
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
