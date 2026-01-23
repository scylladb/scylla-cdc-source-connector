package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaCompositePkNewRecordStateConnectorIT
    extends ScyllaCompositePkBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsert(TestInfo testInfo) {
    return new String[] {
      """
        {
          "pk1": 1,
          "pk2": "%s",
          "pk3": "%s",
          "pk4": 10,
          "value_text": "first",
          "value_int": 100
        }
        """
          .formatted(PK2_VALUE, PK3_VALUE)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdate(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "pk1": 1,
          "pk2": "%s",
          "pk3": "%s",
          "pk4": 10,
          "value_text": "second",
          "value_int": 200
        }
        """
          .formatted(PK2_VALUE, PK3_VALUE)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      """
        {
          "pk1": 1,
          "pk2": "%s",
          "pk3": "%s",
          "pk4": 10,
          "value_text": "first",
          "value_int": 100
        }
        """
          .formatted(PK2_VALUE, PK3_VALUE)
    };
  }
}
