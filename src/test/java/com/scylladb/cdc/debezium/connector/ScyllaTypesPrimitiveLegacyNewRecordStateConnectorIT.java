package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildLegacyScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration test for legacy format with ScyllaExtractNewRecordState transform.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Legacy format Cell structs ({"value": <actual_value>}) are properly unwrapped
 *   <li>The output matches the advanced format's NewRecordState output (flat values)
 *   <li>PK columns are passed through unchanged (they're already flat in legacy format)
 * </ul>
 */
public class ScyllaTypesPrimitiveLegacyNewRecordStateConnectorIT
    extends ScyllaTypesPrimitiveBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildLegacyScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  // Expected outputs should match ScyllaTypesPrimitiveNewRecordStateConnectorIT
  // since Cell structs are unwrapped to flat values

  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
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
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE)
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record: NewRecordState extracts just the "after" value, flattened
      """
        {
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
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE),
      // DELETE record: NewRecordState produces null (tombstone) when after is null
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToNil(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": null
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": "888888888"
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": "888888888"
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": "888888888"
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": null
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": "888888888"
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": null
        }
        """
          .formatted(pk)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
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
          "varint_col": "888888888"
        }
        """
          .formatted(pk)
    };
  }
}
