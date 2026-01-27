package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPk1FromJson;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaCompositePkNewRecordStateConnectorIT
    extends ScyllaCompositePkBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractPk1FromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractPk1FromJson(key);
  }

  @Override
  String[] expectedInsert(int pk1) {
    return new String[] {
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
          .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE)
    };
  }

  @Override
  String[] expectedUpdate(int pk1) {
    return new String[] {
      // INSERT record: NewRecordState extracts flat "after" data
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
      // UPDATE record: NewRecordState extracts flat "after" data
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
          .formatted(pk1, PK2_VALUE, PK3_VALUE, PK4_VALUE)
    };
  }

  @Override
  String[] expectedDelete(int pk1) {
    return new String[] {
      // INSERT record: NewRecordState extracts flat "after" data
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
      // DELETE record: tombstone
      null
    };
  }
}
