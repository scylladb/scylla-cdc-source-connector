package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesPrimitivePlainConnectorIT extends ScyllaTypesPrimitiveBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    int pk = extractIdFromKeyField(value);
    if (pk != -1) {
      return pk;
    }
    // Fallback to extracting from after/before
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "some text",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record: before is null, after has full postimage
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "delete me",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // DELETE record: This table has only partition key (no clustering key), so DELETE
      // becomes PARTITION_DELETE. Scylla doesn't send preimage for PARTITION_DELETE,
      // so "before" is null. The "key" field contains the PK values.
      """
        {
          "before": null,
          "after": null,
          "key": {"id": %d},
          "op": "d",
          "source": %s
        }
        """
          .formatted(pk, expectedSource()),
      // Tombstone record: sent by Debezium after DELETE for log compaction
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToNil(int pk) {
    return new String[] {
      // INSERT record: before is null (row didn't exist), after has full postimage
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      // Note: preimage only contains OLD values of columns that were MODIFIED
      // Unchanged columns (untouched_*) have null values in the preimage
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null (row didn't exist), after has full postimage
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      // Note: preimage only contains OLD values of columns that were MODIFIED
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "",
            "time_col": 3723456000000,
            "timestamp_col": 1718067723456,
            "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 6,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
            "varchar_col": "",
            "varint_col": "888888888",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record: before is null (row didn't exist), after has full postimage
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii2",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "value2",
            "time_col": 3723456000000,
            "timestamp_col": 1718067723456,
            "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 6,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
            "varchar_col": "varchar text 2",
            "varint_col": "888888888",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns (other cols were nil)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (columns being modified were null), after has postimage
      // Note: columns being updated from null have no old value in preimage
      """
        {
          "before": {
            "id": %d
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "varchar text",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (columns being modified were null), after has postimage
      """
        {
          "before": {
            "id": %d
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "",
            "time_col": 3723456000000,
            "timestamp_col": 1718067723456,
            "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 6,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
            "varchar_col": "",
            "varint_col": "888888888",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (columns being modified were null), after has postimage
      // Note: Setting null to null doesn't produce meaningful preimage data
      """
        {
          "before": {
            "id": %d
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii2",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "value2",
            "time_col": 3723456000000,
            "timestamp_col": 1718067723456,
            "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 6,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
            "varchar_col": "varchar text 2",
            "varint_col": "888888888",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource()),
      // UPDATE record: before has preimage (only modified columns), after has postimage
      """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "",
            "time_col": 45296789000000,
            "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
            "varchar_col": "",
            "varint_col": "999999999"
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "",
            "time_col": 3723456000000,
            "timestamp_col": 1718067723456,
            "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
            "tinyint_col": 6,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
            "varchar_col": "",
            "varint_col": "888888888",
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }
}
