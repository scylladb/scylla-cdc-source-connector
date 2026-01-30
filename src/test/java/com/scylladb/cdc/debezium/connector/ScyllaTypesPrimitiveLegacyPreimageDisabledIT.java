package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildLegacyPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for primitive types replication using the legacy output format with preimages
 * disabled.
 *
 * <p>In legacy format, non-PK columns are wrapped in a Cell struct: {@code {"value":
 * <actual_value>}}
 *
 * <p>With preimages disabled (experimental.preimages.enabled=false), the "before" field will be
 * null on CREATE and UPDATE operations. For DELETE operations, "before" will contain only the key
 * fields from the change itself.
 */
public class ScyllaTypesPrimitiveLegacyPreimageDisabledIT
    extends ScyllaTypesPrimitiveBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildLegacyPlainConnector(connectorName, tableName, false);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // Legacy format doesn't include key in value, extract from after object
    int pk = extractIdFromAfter(value);
    if (pk != -1) {
      return pk;
    }
    // For DELETE records, after is null but before has the id
    pk = extractIdFromBefore(value);
    if (pk != -1) {
      return pk;
    }
    // Fallback to extracting from root level
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
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record
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
            "text_col": {"value": "delete me"},
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
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              expectedSource()),
      // DELETE record: "before" contains all columns with null values for non-key columns
      """
        {
          "before": {
            "id": %d,
            "ascii_col": null,
            "bigint_col": null,
            "blob_col": null,
            "boolean_col": null,
            "date_col": null,
            "decimal_col": null,
            "double_col": null,
            "duration_col": null,
            "float_col": null,
            "inet_col": null,
            "int_col": null,
            "smallint_col": null,
            "text_col": null,
            "time_col": null,
            "timestamp_col": null,
            "timeuuid_col": null,
            "tinyint_col": null,
            "uuid_col": null,
            "varchar_col": null,
            "varint_col": null,
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "after": null,
          "op": "d",
          "source": %s
        }
        """
          .formatted(pk, expectedSource()),
      // Tombstone record
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToNil(int pk) {
    return new String[] {
      // INSERT record
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
            "text_col": {"value": "value"},
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
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              expectedSource()),
      // UPDATE record: before is null (no preimage), after has postimage
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": null},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // INSERT record
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
            "text_col": {"value": "value"},
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
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": "888888888"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
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
            "text_col": {"value": "value"},
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
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": "888888888"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(int pk) {
    return new String[] {
      // INSERT record: before is null, after has all columns (most are null)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": null,
            "bigint_col": null,
            "blob_col": null,
            "boolean_col": null,
            "date_col": null,
            "decimal_col": null,
            "double_col": null,
            "duration_col": null,
            "float_col": null,
            "inet_col": null,
            "int_col": null,
            "smallint_col": null,
            "text_col": null,
            "time_col": null,
            "timestamp_col": null,
            "timeuuid_col": null,
            "tinyint_col": null,
            "uuid_col": null,
            "varchar_col": null,
            "varint_col": null,
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
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
            "text_col": {"value": "value"},
            "time_col": {"value": 45296789000000},
            "timestamp_col": {"value": 1718022896789},
            "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
            "tinyint_col": {"value": 5},
            "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
            "varchar_col": {"value": "varchar text"},
            "varint_col": {"value": "999999999"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null, after has all columns (most are null)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": null,
            "bigint_col": null,
            "blob_col": null,
            "boolean_col": null,
            "date_col": null,
            "decimal_col": null,
            "double_col": null,
            "duration_col": null,
            "float_col": null,
            "inet_col": null,
            "int_col": null,
            "smallint_col": null,
            "text_col": null,
            "time_col": null,
            "timestamp_col": null,
            "timeuuid_col": null,
            "tinyint_col": null,
            "uuid_col": null,
            "varchar_col": null,
            "varint_col": null,
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": "888888888"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(int pk) {
    return new String[] {
      // INSERT record: before is null, after has all columns (most are null)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": null,
            "bigint_col": null,
            "blob_col": null,
            "boolean_col": null,
            "date_col": null,
            "decimal_col": null,
            "double_col": null,
            "duration_col": null,
            "float_col": null,
            "inet_col": null,
            "int_col": null,
            "smallint_col": null,
            "text_col": null,
            "time_col": null,
            "timestamp_col": null,
            "timeuuid_col": null,
            "tinyint_col": null,
            "uuid_col": null,
            "varchar_col": null,
            "varint_col": null,
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": null},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": {"value": ""},
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
            "text_col": {"value": ""},
            "time_col": {"value": 45296789000000},
            "timestamp_col": {"value": 1718022896789},
            "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
            "tinyint_col": {"value": 5},
            "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
            "varchar_col": {"value": ""},
            "varint_col": {"value": "999999999"},
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": "888888888"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": {"value": ""},
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
            "text_col": {"value": ""},
            "time_col": {"value": 45296789000000},
            "timestamp_col": {"value": 1718022896789},
            "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
            "tinyint_col": {"value": 5},
            "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
            "varchar_col": {"value": ""},
            "varint_col": {"value": "999999999"},
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": null},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": {"value": ""},
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
            "text_col": {"value": ""},
            "time_col": {"value": 45296789000000},
            "timestamp_col": {"value": 1718022896789},
            "timeuuid_col": {"value": "81d4a030-4632-11f0-9484-409dd8f36eba"},
            "tinyint_col": {"value": 5},
            "uuid_col": {"value": "453662fa-db4b-4938-9033-d8523c0a371c"},
            "varchar_col": {"value": ""},
            "varint_col": {"value": "999999999"},
            "untouched_text": {"value": "%s"},
            "untouched_int": {"value": %d},
            "untouched_boolean": {"value": %s},
            "untouched_uuid": {"value": "%s"}
          },
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
              expectedSource()),
      // UPDATE record: before is null (no preimage)
      """
        {
          "before": null,
          "after": {
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
            "varint_col": {"value": "888888888"},
            "untouched_text": null,
            "untouched_int": null,
            "untouched_boolean": null,
            "untouched_uuid": null
          },
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, expectedSource())
    };
  }
}
