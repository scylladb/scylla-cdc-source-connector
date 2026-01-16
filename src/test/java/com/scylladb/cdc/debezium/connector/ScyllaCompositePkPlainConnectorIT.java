package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaCompositePkPlainConnectorIT extends ScyllaCompositePkBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

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
