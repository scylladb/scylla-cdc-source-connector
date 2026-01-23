package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaCompositePkAvroConnectorIT
    extends ScyllaCompositePkBase<GenericRecord, GenericRecord> {

  @BeforeAll
  @Override
  public void setupSuite(TestInfo testInfo) {
    Assumptions.assumeTrue(
        KAFKA_PROVIDER == KafkaProvider.CONFLUENT, "Avro tests require Confluent Kafka provider");
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "Avro tests require distributed mode, otherwise Avro converter is not available");
    super.setupSuite(testInfo);
  }

  @Override
  KafkaConsumer<GenericRecord, GenericRecord> buildConsumer(
      String connectorName, String tableName) {
    return buildAvroConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(GenericRecord value) {
    return extractPk1FromRecord(value);
  }

  @Override
  protected int extractPkFromKey(GenericRecord key) {
    return extractPk1FromRecord(key);
  }

  private int extractPk1FromRecord(GenericRecord record) {
    if (record == null) {
      return -1;
    }
    // Try to get "after" field first (standard Debezium envelope)
    if (record.getSchema().getField("after") != null) {
      Object after = record.get("after");
      if (after instanceof GenericRecord) {
        GenericRecord afterRecord = (GenericRecord) after;
        if (afterRecord.getSchema().getField("pk1") != null) {
          Object pk1 = afterRecord.get("pk1");
          if (pk1 instanceof Number) {
            return ((Number) pk1).intValue();
          }
        }
      }
    }
    // Try "before" field (for delete operations)
    if (record.getSchema().getField("before") != null) {
      Object before = record.get("before");
      if (before instanceof GenericRecord) {
        GenericRecord beforeRecord = (GenericRecord) before;
        if (beforeRecord.getSchema().getField("pk1") != null) {
          Object pk1 = beforeRecord.get("pk1");
          if (pk1 instanceof Number) {
            return ((Number) pk1).intValue();
          }
        }
      }
    }
    // Fallback to direct "pk1" field (for keys)
    if (record.getSchema().getField("pk1") != null) {
      Object pk1 = record.get("pk1");
      if (pk1 instanceof Number) {
        return ((Number) pk1).intValue();
      }
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
