package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaCompositePkAvroConnectorIT
    extends ScyllaCompositePkBase<GenericRecord, GenericRecord> {

  /** Skips Avro tests unless Confluent distributed mode is available. */
  @BeforeAll
  static void checkKafkaProvider() {
    Assumptions.assumeTrue(
        KAFKA_PROVIDER == KafkaProvider.CONFLUENT, "Avro tests require Confluent Kafka provider");
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "Avro tests require distributed mode, otherwise Avro converter is not available");
  }

  /** {@inheritDoc} */
  @Override
  KafkaConsumer<GenericRecord, GenericRecord> buildConsumer(
      String connectorName, String tableName) {
    return buildAvroConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsert(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
          "c",
          "null",
          """
                {
                  "pk1": 1,
                  "pk2": "%s",
                  "pk3": "%s",
                  "pk4": 10,
                  "value_text": {"value": "first"},
                  "value_int": {"value": 100}
                }
                """
              .formatted(PK2_VALUE, PK3_VALUE))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdate(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
                {
                  "pk1": 1,
                  "pk2": "%s",
                  "pk3": "%s",
                  "pk4": 10,
                  "value_text": {"value": "second"},
                  "value_int": {"value": 200}
                }
                """
              .formatted(PK2_VALUE, PK3_VALUE))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "d",
          """
                {
                  "pk1": 1,
                  "pk2": "%s",
                  "pk3": "%s",
                  "pk4": 10
                }
                """
              .formatted(PK2_VALUE, PK3_VALUE),
          "null"),
      null
    };
  }
}
