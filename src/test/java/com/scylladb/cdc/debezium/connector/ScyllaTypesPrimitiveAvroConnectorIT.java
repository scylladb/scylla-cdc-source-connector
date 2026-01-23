package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesPrimitiveAvroConnectorIT
    extends ScyllaTypesPrimitiveBase<GenericRecord, GenericRecord> {

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
    return extractIdFromRecord(value);
  }

  @Override
  protected int extractPkFromKey(GenericRecord key) {
    return extractIdFromRecord(key);
  }

  private int extractIdFromRecord(GenericRecord record) {
    if (record == null) {
      return -1;
    }
    // Try to get "after" field first (standard Debezium envelope)
    if (record.getSchema().getField("after") != null) {
      Object after = record.get("after");
      if (after instanceof GenericRecord) {
        GenericRecord afterRecord = (GenericRecord) after;
        if (afterRecord.getSchema().getField("id") != null) {
          Object id = afterRecord.get("id");
          if (id instanceof Number) {
            return ((Number) id).intValue();
          }
        }
      }
    }
    // Try "before" field (for delete operations)
    if (record.getSchema().getField("before") != null) {
      Object before = record.get("before");
      if (before instanceof GenericRecord) {
        GenericRecord beforeRecord = (GenericRecord) before;
        if (beforeRecord.getSchema().getField("id") != null) {
          Object id = beforeRecord.get("id");
          if (id instanceof Number) {
            return ((Number) id).intValue();
          }
        }
      }
    }
    // Fallback to direct "id" field (for keys)
    if (record.getSchema().getField("id") != null) {
      Object id = record.get("id");
      if (id instanceof Number) {
        return ((Number) id).intValue();
      }
    }
    return -1;
  }

  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": {"value": "ascii"},
            "bigint_col": {"value": 1234567890123},
            "blob_col": {"value": "Êþº¾"},
            "boolean_col": {"value": true},
            "date_col": {"value": 19884},
            "decimal_col": {"value": "12345.67"},
            "double_col": {"value": 3.14159},
            "duration_col": {"value": "1d12h30m"},
            "float_col": {"value": 2.71828},
            "inet_col": {"value": "127.0.0.1"},
            "int_col": {"value": 42},
            "smallint_col": {"value": 7},
            "text_col": {"value": "some text"},
            "time_col": {"value": 45296789000000},
            "timestamp_col": {"value": 1718022896789},
            "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
            "tinyint_col": {"value": 5},
            "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
            "varchar_col": {"value": "varchar text"},
            "varint_col": {"value": "999999999"},
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
          "op": "c",
          "source": {
            "connector": "scylla",
            "name": "%s",
            "snapshot": "false",
            "db": "%s",
            "keyspace_name": "%s",
            "table_name": "%s"
          }
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              getSuiteConnectorName(),
              getSuiteKeyspaceName(),
              getSuiteKeyspaceName(),
              getSuiteTableName())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "d",
          """
            {
              "id": %d
            }
            """
              .formatted(pk),
          "null"),
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToNil(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": null},
              "bigint_col": {"value": null},
              "blob_col": {"value": null},
              "boolean_col": {"value": null},
              "date_col": {"value": null},
              "decimal_col": {"value": null},
              "double_col": {"value": null},
              "duration_col": {"value": null},
              "float_col": {"value": null},
              "inet_col": {"value": null},
              "int_col": {"value": null},
              "smallint_col": {"value": null},
              "text_col": {"value": null},
              "time_col": {"value": null},
              "timestamp_col": {"value": null},
              "timeuuid_col": {"value": null},
              "tinyint_col": {"value": null},
              "uuid_col": {"value": null},
              "varchar_col": {"value": null},
              "varint_col": {"value": null}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "Þ­¾ï"},
              "boolean_col": {"value": false},
              "date_col": {"value": 19885},
              "decimal_col": {"value": "98765.43"},
              "double_col": {"value": 2.71828},
              "duration_col": {"value": "2d1h"},
              "float_col": {"value": 1.41421},
              "inet_col": {"value": "127.0.0.2"},
              "int_col": {"value": 43},
              "smallint_col": {"value": 8},
              "text_col": {"value": ""},
              "time_col": {"value": 3723456000000},
              "timestamp_col": {"value": 1718067723456},
              "timeuuid_col": {"value": "81d4a031-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 6},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371d"},
              "varchar_col": {"value": ""},
              "varint_col": {"value": "888888888"}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": "ascii2"},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "Þ­¾ï"},
              "boolean_col": {"value": false},
              "date_col": {"value": 19885},
              "decimal_col": {"value": "98765.43"},
              "double_col": {"value": 2.71828},
              "duration_col": {"value": "2d1h"},
              "float_col": {"value": 1.41421},
              "inet_col": {"value": "127.0.0.2"},
              "int_col": {"value": 43},
              "smallint_col": {"value": 8},
              "text_col": {"value": "value2"},
              "time_col": {"value": 3723456000000},
              "timestamp_col": {"value": 1718067723456},
              "timeuuid_col": {"value": "81d4a031-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 6},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371d"},
              "varchar_col": {"value": "varchar text 2"},
              "varint_col": {"value": "888888888"}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": "ascii"},
              "bigint_col": {"value": 1234567890123},
              "blob_col": {"value": "Êþº¾"},
              "boolean_col": {"value": true},
              "date_col": {"value": 19884},
              "decimal_col": {"value": "12345.67"},
              "double_col": {"value": 3.14159},
              "duration_col": {"value": "1d12h30m"},
              "float_col": {"value": 2.71828},
              "inet_col": {"value": "127.0.0.1"},
              "int_col": {"value": 42},
              "smallint_col": {"value": 7},
              "text_col": {"value": "value"},
              "time_col": {"value": 45296789000000},
              "timestamp_col": {"value": 1718022896789},
              "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 5},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
              "varchar_col": {"value": "varchar text"},
              "varint_col": {"value": "999999999"}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "Þ­¾ï"},
              "boolean_col": {"value": false},
              "date_col": {"value": 19885},
              "decimal_col": {"value": "98765.43"},
              "double_col": {"value": 2.71828},
              "duration_col": {"value": "2d1h"},
              "float_col": {"value": 1.41421},
              "inet_col": {"value": "127.0.0.2"},
              "int_col": {"value": 43},
              "smallint_col": {"value": 8},
              "text_col": {"value": ""},
              "time_col": {"value": 3723456000000},
              "timestamp_col": {"value": 1718067723456},
              "timeuuid_col": {"value": "81d4a031-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 6},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371d"},
              "varchar_col": {"value": ""},
              "varint_col": {"value": "888888888"}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": null},
              "bigint_col": {"value": null},
              "blob_col": {"value": null},
              "boolean_col": {"value": null},
              "date_col": {"value": null},
              "decimal_col": {"value": null},
              "double_col": {"value": null},
              "duration_col": {"value": null},
              "float_col": {"value": null},
              "inet_col": {"value": null},
              "int_col": {"value": null},
              "smallint_col": {"value": null},
              "text_col": {"value": null},
              "time_col": {"value": null},
              "timestamp_col": {"value": null},
              "timeuuid_col": {"value": null},
              "tinyint_col": {"value": null},
              "uuid_col": {"value": null},
              "varchar_col": {"value": null},
              "varint_col": {"value": null}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": "ascii2"},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "Þ­¾ï"},
              "boolean_col": {"value": false},
              "date_col": {"value": 19885},
              "decimal_col": {"value": "98765.43"},
              "double_col": {"value": 2.71828},
              "duration_col": {"value": "2d1h"},
              "float_col": {"value": 1.41421},
              "inet_col": {"value": "127.0.0.2"},
              "int_col": {"value": 43},
              "smallint_col": {"value": 8},
              "text_col": {"value": "value"},
              "time_col": {"value": 3723456000000},
              "timestamp_col": {"value": 1718067723456},
              "timeuuid_col": {"value": "81d4a031-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 6},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371d"},
              "varchar_col": {"value": "varchar text 2"},
              "varint_col": {"value": "888888888"}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": null},
              "bigint_col": {"value": null},
              "blob_col": {"value": null},
              "boolean_col": {"value": null},
              "date_col": {"value": null},
              "decimal_col": {"value": null},
              "double_col": {"value": null},
              "duration_col": {"value": null},
              "float_col": {"value": null},
              "inet_col": {"value": null},
              "int_col": {"value": null},
              "smallint_col": {"value": null},
              "text_col": {"value": null},
              "time_col": {"value": null},
              "timestamp_col": {"value": null},
              "timeuuid_col": {"value": null},
              "tinyint_col": {"value": null},
              "uuid_col": {"value": null},
              "varchar_col": {"value": null},
              "varint_col": {"value": null}
            }
            """
              .formatted(pk))
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "Þ­¾ï"},
              "boolean_col": {"value": false},
              "date_col": {"value": 19885},
              "decimal_col": {"value": "98765.43"},
              "double_col": {"value": 2.71828},
              "duration_col": {"value": "2d1h"},
              "float_col": {"value": 1.41421},
              "inet_col": {"value": "127.0.0.2"},
              "int_col": {"value": 43},
              "smallint_col": {"value": 8},
              "text_col": {"value": ""},
              "time_col": {"value": 3723456000000},
              "timestamp_col": {"value": 1718067723456},
              "timeuuid_col": {"value": "81d4a031-4632-11f0-9484-409dd8f36eba"},
              "tinyint_col": {"value": 6},
              "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371d"},
              "varchar_col": {"value": ""},
              "varint_col": {"value": "888888888"}
            }
            """
              .formatted(pk))
    };
  }
}
