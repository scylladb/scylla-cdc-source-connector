package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesPrimitivePlainConnectorIT extends ScyllaTypesPrimitiveBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsert(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "ascii_col": {"value": "ascii"},
            "bigint_col": {"value": 1234567890123},
            "blob_col": {"value": "yv66vg=="},
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
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
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
              "id": 1
            }
            """,
          "null"),
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToNil(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "3q2+7w=="},
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": "ascii2"},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "3q2+7w=="},
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": "ascii"},
              "bigint_col": {"value": 1234567890123},
              "blob_col": {"value": "yv66vg=="},
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "3q2+7w=="},
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": "ascii2"},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "3q2+7w=="},
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
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
            """)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "ascii_col": {"value": ""},
              "bigint_col": {"value": 1234567890124},
              "blob_col": {"value": "3q2+7w=="},
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
            """)
    };
  }
}
