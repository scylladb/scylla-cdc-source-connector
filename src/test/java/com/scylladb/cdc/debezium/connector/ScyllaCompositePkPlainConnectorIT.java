package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaCompositePkPlainConnectorIT extends ScyllaCompositePkBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractPk1FromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractPk1FromJson(key);
  }

  private int extractPk1FromJson(String json) {
    // Parse JSON to extract "pk1" from "after" or root level
    if (json == null) {
      return -1;
    }
    int pk1Index = json.indexOf("\"pk1\":");
    if (pk1Index == -1) {
      return -1;
    }
    int start = pk1Index + 6;
    while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
      start++;
    }
    int end = start;
    while (end < json.length()
        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
      end++;
    }
    if (end > start) {
      return Integer.parseInt(json.substring(start, end));
    }
    return -1;
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
              "value_text": {"value": "first"},
              "value_int": {"value": 100}
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE))
    };
  }

  @Override
  String[] expectedUpdate(int pk1) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d,
              "value_text": {"value": "second"},
              "value_int": {"value": 200}
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE))
    };
  }

  @Override
  String[] expectedDelete(int pk1) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "d",
          """
            {
              "pk1": %d,
              "pk2": "%s",
              "pk3": "%s",
              "pk4": %d
            }
            """
              .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE),
          "null"),
      null
    };
  }
}
