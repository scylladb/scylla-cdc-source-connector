package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * NewRecordState-based integration tests for all Scylla types replication. This class extends
 * {@link ScyllaTypesAllBase} and provides NewRecordState-specific expected JSON (flattened output
 * without the Debezium envelope) for all test scenarios.
 *
 * <p>The NewRecordState transformer extracts just the "after" values from the Debezium envelope,
 * producing a simpler, flattened output format. DELETE operations produce null (tombstone) records.
 */
public class ScyllaTypesAllNewRecordStateConnectorIT extends ScyllaTypesAllBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Override
  protected String blobValueSet1() {
    return "yv66vg=="; // base64 for 0xCAFEBABE
  }

  @Override
  protected String blobValueSet2() {
    return "3q2+7w=="; // base64 for 0xDEADBEEF
  }

  // ===================== Helper Methods for Building Expected JSON =====================
  // NewRecordState produces flattened output (just the "after" values, no envelope)
  // Uses shared methods from ScyllaTypesAllBase for common column fragments.

  /** Returns values for all non-frozen collection columns (initial values). */
  private String nonFrozenCollectionsColumnsValues() {
    return """
        "list_col": [10, 20, 30],
        "set_col": ["x", "y", "z"],
        "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
        "map_ascii_col": [{"key": "ascii_key", "value": "ascii_value"}],
        "map_bigint_col": [{"key": 1234567890123, "value": "bigint_value"}],
        "map_boolean_col": [{"key": true, "value": "boolean_value"}],
        "map_date_col": [{"key": 19884, "value": "date_value"}],
        "map_decimal_col": [{"key": "12345.67", "value": "decimal_value"}],
        "map_double_col": [{"key": 3.14159, "value": "double_value"}],
        "map_float_col": [{"key": 2.71828, "value": "float_value"}],
        "map_int_col": [{"key": 42, "value": "int_value"}],
        "map_inet_col": [{"key": "127.0.0.1", "value": "inet_value"}],
        "map_smallint_col": [{"key": 7, "value": "smallint_value"}],
        "map_text_col": [{"key": "text_key", "value": "text_value"}],
        "map_time_col": [{"key": 45296789000000, "value": "time_value"}],
        "map_timestamp_col": [{"key": 1718022896789, "value": "timestamp_value"}],
        "map_timeuuid_col": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}],
        "map_tinyint_col": [{"key": 5, "value": "tinyint_value"}],
        "map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}],
        "map_varchar_col": [{"key": "varchar_key", "value": "varchar_value"}],
        "map_varint_col": [{"key": "999999999", "value": "varint_value"}],
        "set_timeuuid_col": ["81d4a030-4632-11f0-9484-409dd8f36eba"]""";
  }

  /** Returns updated values for all non-frozen collection columns. */
  private String nonFrozenCollectionsColumnsValuesUpdated() {
    return """
        "list_col": [40, 50, 60],
        "set_col": ["a", "b", "c"],
        "map_col": [{"key": 30, "value": "thirty"}, {"key": 40, "value": "forty"}],
        "map_ascii_col": [{"key": "ascii_key_2", "value": "ascii_value_2"}],
        "map_bigint_col": [{"key": 1234567890124, "value": "bigint_value_2"}],
        "map_boolean_col": [{"key": false, "value": "boolean_value_2"}],
        "map_date_col": [{"key": 19885, "value": "date_value_2"}],
        "map_decimal_col": [{"key": "98765.43", "value": "decimal_value_2"}],
        "map_double_col": [{"key": 2.71828, "value": "double_value_2"}],
        "map_float_col": [{"key": 1.41421, "value": "float_value_2"}],
        "map_int_col": [{"key": 43, "value": "int_value_2"}],
        "map_inet_col": [{"key": "127.0.0.2", "value": "inet_value_2"}],
        "map_smallint_col": [{"key": 8, "value": "smallint_value_2"}],
        "map_text_col": [{"key": "text_key_2", "value": "text_value_2"}],
        "map_time_col": [{"key": 3723456000000, "value": "time_value_2"}],
        "map_timestamp_col": [{"key": 1718067723456, "value": "timestamp_value_2"}],
        "map_timeuuid_col": [{"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value_2"}],
        "map_tinyint_col": [{"key": 6, "value": "tinyint_value_2"}],
        "map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371d", "value": "uuid_value_2"}],
        "map_varchar_col": [{"key": "varchar_key_2", "value": "varchar_value_2"}],
        "map_varint_col": [{"key": "888888888", "value": "varint_value_2"}],
        "set_timeuuid_col": ["81d4a031-4632-11f0-9484-409dd8f36eba"]""";
  }

  // udtColumnsNull() and udtColumnsValues() are inherited from ScyllaTypesAllBase

  /** Returns null values for primitive columns (flattened). */
  private String primitiveColumnsNull(int pk) {
    return """
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
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns values for primitive columns (set 1) with base64-encoded blob. */
  private String primitiveColumnsValues(int pk, String textValue) {
    return """
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
        "text_col": "%s",
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
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            textValue,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns values for primitive columns (set 2 - updated values) with base64-encoded blob. */
  private String primitiveColumnsValuesSet2(int pk, String textValue) {
    return """
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
        "text_col": "%s",
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
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            textValue,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns empty string values for primitive columns. */
  private String primitiveColumnsEmpty(int pk) {
    return """
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
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns empty string values for primitive columns (set 2). */
  private String primitiveColumnsEmptySet2(int pk) {
    return """
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
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Helper to build a flattened JSON record with all columns. */
  private String flattenedRecord(
      String primitiveColumns,
      String frozenCollectionsColumns,
      String nonFrozenCollectionsColumns,
      String udtColumns) {
    return """
        {
          %s,
          %s,
          %s,
          %s
        }
        """
        .formatted(
            primitiveColumns, frozenCollectionsColumns, nonFrozenCollectionsColumns, udtColumns);
  }

  /** Helper for frozen collections tests - returns primitive columns with null values. */
  private String frozenAfterColumnsWithValues(int pk) {
    return """
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
        "untouched_uuid": null"""
        .formatted(pk);
  }

  // ===================== Primitive Expected Methods =====================
  // NewRecordState produces flattened output (just the "after" values)

  @Override
  String[] expectedPrimitiveInsert(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsValues(pk, "some text"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveDelete(int pk) {
    return new String[] {
      // INSERT record (flattened)
      flattenedRecord(
          primitiveColumnsValues(pk, "delete me"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      // DELETE: NewRecordState produces null when after is null
      null
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToNil(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsValues(pk, "value"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToEmpty(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsValues(pk, "value"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsEmptySet2(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToValue(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsValues(pk, "value"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsValuesSet2(pk, "value2"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToValue(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsValues(pk, "value"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToEmpty(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsEmptySet2(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToNil(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToValue(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsEmpty(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsValuesSet2(pk, "value2"),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToNil(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsEmpty(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsNull(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      flattenedRecord(
          primitiveColumnsEmpty(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          primitiveColumnsEmptySet2(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  // ===================== Frozen Collections Expected Methods =====================

  @Override
  String[] expectedFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithEmpty(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsEmpty(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsDelete(int pk) {
    return new String[] {
      // INSERT record (flattened)
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      // DELETE: NewRecordState produces null when after is null
      null
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValuesUpdated(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsEmpty(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsEmpty(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsValues(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  // ===================== Non-Frozen Collections Expected Methods =====================

  @Override
  String[] expectedNonFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          %s,
          %s,
          "list_col": [10],
          "set_col": ["x"],
          "map_col": [{"key": 10, "value": "ten"}],
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      // DELETE: NewRecordState produces null
      null
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListAddElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": [10, 20],
          "set_col": null,
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": [10, 20, 30],
          "set_col": null,
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetAddElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": ["x", "y"],
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": ["x", "y", "z"],
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapAddElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}],
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListRemoveElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": [10, 20, 30],
          "set_col": null,
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": [10, 30],
          "set_col": null,
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetRemoveElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": ["x", "y", "z"],
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": ["x", "z"],
          "map_col": null,
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapRemoveElement(int pk) {
    return new String[] {
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull()),
      """
        {
          %s,
          %s,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 20, "value": "twenty"}],
          "map_ascii_col": null,
          "map_bigint_col": null,
          "map_boolean_col": null,
          "map_date_col": null,
          "map_decimal_col": null,
          "map_double_col": null,
          "map_float_col": null,
          "map_int_col": null,
          "map_inet_col": null,
          "map_smallint_col": null,
          "map_text_col": null,
          "map_time_col": null,
          "map_timestamp_col": null,
          "map_timeuuid_col": null,
          "map_tinyint_col": null,
          "map_uuid_col": null,
          "map_varchar_col": null,
          "map_varint_col": null,
          "set_timeuuid_col": null,
          %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk), frozenCollectionsColumnsNull(), udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValuesUpdated(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    // Empty non-frozen collections become null in Scylla
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    // Empty non-frozen collections are stored as null in Scylla
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsValues(),
          udtColumnsNull())
    };
  }

  // ===================== UDT Expected Methods =====================

  @Override
  String[] expectedUdtInsert(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues())
    };
  }

  @Override
  String[] expectedUdtInsertWithNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedUdtInsertWithEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedUdtDelete(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues()),
      // DELETE: NewRecordState produces null
      null
    };
  }

  /** UDT values for frozen update test (nf_udt has same value as frozen_udt). */
  private String udtColumnsValuesForFrozenUpdate() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": {"a": 42, "b": "foo"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  /** UDT values after frozen update. */
  private String udtColumnsValuesAfterFrozenUpdate() {
    return """
        "frozen_udt": {"a": 99, "b": "updated"},
        "nf_udt": {"a": 77, "b": "foo"},
        "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "frozen_udt_with_list": {"l": [4, 5, 6]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["d", "e"]},
        "nf_udt_with_set": {"s": ["d", "e"]}""";
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesForFrozenUpdate()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesAfterFrozenUpdate())
    };
  }

  /** UDT values for value-to-null test (nf_udt is null). */
  private String udtColumnsValuesForValueToNull() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": null,
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesForValueToNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromNullToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromEmptyToValue(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsNull()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues())
    };
  }

  /** UDT values for non-frozen field test (frozen_udt is null). */
  private String udtColumnsValuesForNonFrozenField() {
    return """
        "frozen_udt": null,
        "nf_udt": {"a": 7, "b": "bar"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  /** UDT values after non-frozen field update. */
  private String udtColumnsValuesAfterNonFrozenFieldUpdate() {
    return """
        "frozen_udt": {"a": 99, "b": "updated"},
        "nf_udt": {"a": 100, "b": "bar"},
        "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "frozen_udt_with_list": {"l": [4, 5, 6]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["d", "e"]},
        "nf_udt_with_set": {"s": ["d", "e"]}""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtField(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesForNonFrozenField()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesAfterNonFrozenFieldUpdate())
    };
  }

  /** UDT values after non-frozen replace update. */
  private String udtColumnsValuesAfterNonFrozenReplace() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": {"a": 99, "b": "updated"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}, {"key": "81d4a033-4632-11f0-9484-409dd8f36eba", "value": "value4"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["d", "e", "f"]}""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesAfterNonFrozenReplace())
    };
  }

  /** UDT values after non-frozen columns set to null. */
  private String udtColumnsValuesAfterNonFrozenToNull() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": null,
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": null,
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": null,
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": null,
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": null""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValues()),
      flattenedRecord(
          frozenAfterColumnsWithValues(pk),
          frozenCollectionsColumnsNull(),
          nonFrozenCollectionsColumnsNull(),
          udtColumnsValuesAfterNonFrozenToNull())
    };
  }
}
