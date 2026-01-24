package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesPrimitivePlainConnectorIT extends ScyllaTypesPrimitiveBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  private int extractIdFromJson(String json) {
    // Parse JSON to extract "id" from "after" or root level
    // Simple parsing for {"after":{"id":N,...}} or {"id":N,...}
    if (json == null) {
      return -1;
    }
    int idIndex = json.indexOf("\"id\":");
    if (idIndex == -1) {
      return -1;
    }
    int start = idIndex + 5;
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
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
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
            """
              .formatted(pk))
    };
  }
}
