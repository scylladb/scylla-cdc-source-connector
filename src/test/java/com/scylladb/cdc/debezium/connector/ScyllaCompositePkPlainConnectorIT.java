package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPk1FromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPk1FromKeyField;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaCompositePkPlainConnectorIT extends ScyllaCompositePkBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractPk1FromKeyField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractPk1FromJson(key);
  }

  @Override
  String[] expectedInsert(int pk1) {
    return new String[] {
      expectedRecord(
          "c",
          "null",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "first",
              "value_int": 100
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          expectedCompositeKey(pk1))
    };
  }

  private String expectedCompositeKey(int pk1) {
    return """
        {"pk1": %d, "pk2": "%s", "pk3": "%s", "pk4": %d}
        """
        .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE);
  }

  @Override
  String[] expectedUpdate(int pk1) {
    return new String[] {
      // INSERT record: before is null, after has full postimage
      expectedRecord(
          "c",
          "null",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "first",
              "value_int": 100
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          expectedCompositeKey(pk1)),
      // UPDATE record: before has preimage, after has postimage
      expectedRecord(
          "u",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "first",
              "value_int": 100
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "second",
              "value_int": 200
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          expectedCompositeKey(pk1))
    };
  }

  @Override
  String[] expectedDelete(int pk1) {
    return new String[] {
      // INSERT record: before is null, after has full postimage
      expectedRecord(
          "c",
          "null",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "first",
              "value_int": 100
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          expectedCompositeKey(pk1)),
      // DELETE record: before has preimage, after is null
      expectedRecord(
          "d",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": "first",
              "value_int": 100
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          "null",
          expectedCompositeKey(pk1)),
      null
    };
  }
}
