package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Consolidated integration tests for all Scylla types replication. Combines tests for primitive
 * types, frozen collections, non-frozen collections, and UDTs into a single test base class.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesAllBase<K, V> extends ScyllaTypesIT<K, V> {

  // ===================== Primitive Constants =====================
  static final String UNTOUCHED_TEXT_VALUE = "untouched text";
  static final int UNTOUCHED_INT_VALUE = 101;
  static final boolean UNTOUCHED_BOOLEAN_VALUE = true;
  static final String UNTOUCHED_UUID_VALUE = "11111111-1111-1111-1111-111111111111";

  // ===================== Abstract Expected Methods - Primitive =====================

  abstract String[] expectedPrimitiveInsert(int pk);

  abstract String[] expectedPrimitiveDelete(int pk);

  abstract String[] expectedPrimitiveUpdateFromValueToNil(int pk);

  abstract String[] expectedPrimitiveUpdateFromValueToEmpty(int pk);

  abstract String[] expectedPrimitiveUpdateFromValueToValue(int pk);

  abstract String[] expectedPrimitiveUpdateFromNilToValue(int pk);

  abstract String[] expectedPrimitiveUpdateFromNilToEmpty(int pk);

  abstract String[] expectedPrimitiveUpdateFromNilToNil(int pk);

  abstract String[] expectedPrimitiveUpdateFromEmptyToValue(int pk);

  abstract String[] expectedPrimitiveUpdateFromEmptyToNil(int pk);

  abstract String[] expectedPrimitiveUpdateFromEmptyToEmpty(int pk);

  // ===================== Abstract Expected Methods - Frozen Collections =====================

  abstract String[] expectedFrozenCollectionsInsertWithValues(int pk);

  abstract String[] expectedFrozenCollectionsInsertWithEmpty(int pk);

  abstract String[] expectedFrozenCollectionsInsertWithNull(int pk);

  abstract String[] expectedFrozenCollectionsDelete(int pk);

  abstract String[] expectedFrozenCollectionsUpdateFromValueToValue(int pk);

  abstract String[] expectedFrozenCollectionsUpdateFromValueToEmpty(int pk);

  abstract String[] expectedFrozenCollectionsUpdateFromValueToNull(int pk);

  abstract String[] expectedFrozenCollectionsUpdateFromEmptyToValue(int pk);

  abstract String[] expectedFrozenCollectionsUpdateFromNullToValue(int pk);

  // ===================== Abstract Expected Methods - Non-Frozen Collections =====================

  abstract String[] expectedNonFrozenCollectionsInsertWithValues(int pk);

  abstract String[] expectedNonFrozenCollectionsInsertWithNull(int pk);

  abstract String[] expectedNonFrozenCollectionsDelete(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateListAddElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateSetAddElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateMapAddElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateListRemoveElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateSetRemoveElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateMapRemoveElement(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateFromValueToValue(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateFromValueToNull(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateFromValueToEmpty(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateFromNullToValue(int pk);

  abstract String[] expectedNonFrozenCollectionsUpdateFromEmptyToValue(int pk);

  // ===================== Abstract Expected Methods - UDT =====================

  abstract String[] expectedUdtInsert(int pk);

  abstract String[] expectedUdtInsertWithNull(int pk);

  abstract String[] expectedUdtInsertWithEmpty(int pk);

  abstract String[] expectedUdtDelete(int pk);

  abstract String[] expectedUdtUpdateFrozenUdtFromValueToValue(int pk);

  abstract String[] expectedUdtUpdateFrozenUdtFromValueToNull(int pk);

  abstract String[] expectedUdtUpdateFrozenUdtFromValueToEmpty(int pk);

  abstract String[] expectedUdtUpdateFrozenUdtFromNullToValue(int pk);

  abstract String[] expectedUdtUpdateFrozenUdtFromEmptyToValue(int pk);

  abstract String[] expectedUdtUpdateNonFrozenUdtField(int pk);

  abstract String[] expectedUdtUpdateNonFrozenUdtFromValueToValue(int pk);

  abstract String[] expectedUdtUpdateNonFrozenUdtFromValueToNull(int pk);

  // ===================== Abstract Methods for Format-Specific Encoding =====================

  /**
   * Returns the blob value encoded for set 1 (0xCAFEBABE). Different implementations use different
   * encodings: Plain/NewRecordState use base64 ("yv66vg=="), Avro uses character encoding ("Êþº¾").
   */
  protected abstract String blobValueSet1();

  /**
   * Returns the blob value encoded for set 2 (0xDEADBEEF). Different implementations use different
   * encodings: Plain/NewRecordState use base64 ("3q2+7w=="), Avro uses character encoding ("Þ­¾ï").
   */
  protected abstract String blobValueSet2();

  // ===================== Shared JSON Fragment Helpers =====================
  // These methods generate JSON fragments that are identical across all implementations.
  // Only blob encoding differs (handled via abstract blobValueSet1/blobValueSet2 methods).

  /** Returns null values for all frozen collection columns. */
  protected String frozenCollectionsColumnsNull() {
    return """
        "frozen_list_col": null,
        "frozen_set_col": null,
        "frozen_map_col": null,
        "frozen_tuple_col": null,
        "frozen_map_ascii_col": null,
        "frozen_map_bigint_col": null,
        "frozen_map_boolean_col": null,
        "frozen_map_date_col": null,
        "frozen_map_decimal_col": null,
        "frozen_map_double_col": null,
        "frozen_map_float_col": null,
        "frozen_map_int_col": null,
        "frozen_map_inet_col": null,
        "frozen_map_smallint_col": null,
        "frozen_map_text_col": null,
        "frozen_map_time_col": null,
        "frozen_map_timestamp_col": null,
        "frozen_map_timeuuid_col": null,
        "frozen_map_tinyint_col": null,
        "frozen_map_uuid_col": null,
        "frozen_map_varchar_col": null,
        "frozen_map_varint_col": null,
        "frozen_map_tuple_key_col": null,
        "frozen_map_udt_key_col": null,
        "frozen_set_timeuuid_col": null""";
  }

  /** Returns values for all frozen collection columns (initial values). */
  protected String frozenCollectionsColumnsValues() {
    return """
        "frozen_list_col": [1, 2, 3],
        "frozen_set_col": ["a", "b", "c"],
        "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
        "frozen_tuple_col": {"field_0": 42, "field_1": "foo"},
        "frozen_map_ascii_col": [{"key": "ascii_key", "value": "ascii_value"}],
        "frozen_map_bigint_col": [{"key": 1234567890123, "value": "bigint_value"}],
        "frozen_map_boolean_col": [{"key": true, "value": "boolean_value"}],
        "frozen_map_date_col": [{"key": 19884, "value": "date_value"}],
        "frozen_map_decimal_col": [{"key": "12345.67", "value": "decimal_value"}],
        "frozen_map_double_col": [{"key": 3.14159, "value": "double_value"}],
        "frozen_map_float_col": [{"key": 2.71828, "value": "float_value"}],
        "frozen_map_int_col": [{"key": 42, "value": "int_value"}],
        "frozen_map_inet_col": [{"key": "127.0.0.1", "value": "inet_value"}],
        "frozen_map_smallint_col": [{"key": 7, "value": "smallint_value"}],
        "frozen_map_text_col": [{"key": "text_key", "value": "text_value"}],
        "frozen_map_time_col": [{"key": 45296789000000, "value": "time_value"}],
        "frozen_map_timestamp_col": [{"key": 1718022896789, "value": "timestamp_value"}],
        "frozen_map_timeuuid_col": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}],
        "frozen_map_tinyint_col": [{"key": 5, "value": "tinyint_value"}],
        "frozen_map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}],
        "frozen_map_varchar_col": [{"key": "varchar_key", "value": "varchar_value"}],
        "frozen_map_varint_col": [{"key": "999999999", "value": "varint_value"}],
        "frozen_map_tuple_key_col": [{"key": {"field_0": 1, "field_1": "tuple_key"}, "value": "tuple_value"}],
        "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}],
        "frozen_set_timeuuid_col": ["81d4a030-4632-11f0-9484-409dd8f36eba"]""";
  }

  /** Returns updated values for all frozen collection columns. */
  protected String frozenCollectionsColumnsValuesUpdated() {
    return """
        "frozen_list_col": [4, 5, 6],
        "frozen_set_col": ["x", "y", "z"],
        "frozen_map_col": [{"key": 3, "value": "three"}, {"key": 4, "value": "four"}],
        "frozen_tuple_col": {"field_0": 99, "field_1": "bar"},
        "frozen_map_ascii_col": [{"key": "ascii_key_2", "value": "ascii_value_2"}],
        "frozen_map_bigint_col": [{"key": 1234567890124, "value": "bigint_value_2"}],
        "frozen_map_boolean_col": [{"key": false, "value": "boolean_value_2"}],
        "frozen_map_date_col": [{"key": 19885, "value": "date_value_2"}],
        "frozen_map_decimal_col": [{"key": "98765.43", "value": "decimal_value_2"}],
        "frozen_map_double_col": [{"key": 2.71828, "value": "double_value_2"}],
        "frozen_map_float_col": [{"key": 1.41421, "value": "float_value_2"}],
        "frozen_map_int_col": [{"key": 43, "value": "int_value_2"}],
        "frozen_map_inet_col": [{"key": "127.0.0.2", "value": "inet_value_2"}],
        "frozen_map_smallint_col": [{"key": 8, "value": "smallint_value_2"}],
        "frozen_map_text_col": [{"key": "text_key_2", "value": "text_value_2"}],
        "frozen_map_time_col": [{"key": 3723456000000, "value": "time_value_2"}],
        "frozen_map_timestamp_col": [{"key": 1718067723456, "value": "timestamp_value_2"}],
        "frozen_map_timeuuid_col": [{"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value_2"}],
        "frozen_map_tinyint_col": [{"key": 6, "value": "tinyint_value_2"}],
        "frozen_map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371d", "value": "uuid_value_2"}],
        "frozen_map_varchar_col": [{"key": "varchar_key_2", "value": "varchar_value_2"}],
        "frozen_map_varint_col": [{"key": "888888888", "value": "varint_value_2"}],
        "frozen_map_tuple_key_col": [{"key": {"field_0": 2, "field_1": "tuple_key_2"}, "value": "tuple_value_2"}],
        "frozen_map_udt_key_col": [{"key": {"a": 2, "b": "udt_key_2"}, "value": "udt_value_2"}],
        "frozen_set_timeuuid_col": ["81d4a031-4632-11f0-9484-409dd8f36eba"]""";
  }

  /** Returns empty values for all frozen collection columns. */
  protected String frozenCollectionsColumnsEmpty() {
    return """
        "frozen_list_col": [],
        "frozen_set_col": [],
        "frozen_map_col": [],
        "frozen_tuple_col": {"field_0": null, "field_1": null},
        "frozen_map_ascii_col": [],
        "frozen_map_bigint_col": [],
        "frozen_map_boolean_col": [],
        "frozen_map_date_col": [],
        "frozen_map_decimal_col": [],
        "frozen_map_double_col": [],
        "frozen_map_float_col": [],
        "frozen_map_int_col": [],
        "frozen_map_inet_col": [],
        "frozen_map_smallint_col": [],
        "frozen_map_text_col": [],
        "frozen_map_time_col": [],
        "frozen_map_timestamp_col": [],
        "frozen_map_timeuuid_col": [],
        "frozen_map_tinyint_col": [],
        "frozen_map_uuid_col": [],
        "frozen_map_varchar_col": [],
        "frozen_map_varint_col": [],
        "frozen_map_tuple_key_col": [],
        "frozen_map_udt_key_col": [],
        "frozen_set_timeuuid_col": []""";
  }

  /** Returns null values for all non-frozen collection columns. */
  protected String nonFrozenCollectionsColumnsNull() {
    return """
        "list_col": null,
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
        "set_timeuuid_col": null""";
  }

  // Note: nonFrozenCollectionsColumnsValues() is NOT shared because implementations
  // use different initial values. Each implementation defines its own version.

  /** Returns null values for all UDT columns. */
  protected String udtColumnsNull() {
    return """
        "frozen_udt": null,
        "nf_udt": null,
        "frozen_nested_udt": null,
        "nf_nested_udt": null,
        "frozen_udt_with_map": null,
        "nf_udt_with_map": null,
        "frozen_udt_with_list": null,
        "nf_udt_with_list": null,
        "frozen_udt_with_set": null,
        "nf_udt_with_set": null""";
  }

  /** Returns values for all UDT columns (initial values). */
  protected String udtColumnsValues() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
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

  /** Returns empty values for all UDT columns. */
  protected String udtColumnsEmpty() {
    return """
        "frozen_udt": {"a": null, "b": null},
        "nf_udt": {"a": null, "b": null},
        "frozen_nested_udt": {"inner": null, "z": null},
        "nf_nested_udt": {"inner": null, "z": null},
        "frozen_udt_with_map": {"m": null},
        "nf_udt_with_map": {"m": null},
        "frozen_udt_with_list": {"l": null},
        "nf_udt_with_list": {"l": null},
        "frozen_udt_with_set": {"s": null},
        "nf_udt_with_set": {"s": null}""";
  }

  // ===================== UDT Creation =====================

  @Override
  protected void createTypesBeforeTable(String keyspaceName) {
    // From ScyllaTypesFrozenCollectionsBase
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".map_key_udt (a int, b text);");

    // From ScyllaTypesUDTBase (inner_udt MUST come before nested_udt)
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".simple_udt (a int, b text);");
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".inner_udt (x int, y text);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_map (m frozen<map<timeuuid, text>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_list (l frozen<list<int>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_set (s frozen<set<text>>);");
  }

  // ===================== Combined Table Schema =====================

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        // Primitive columns (20 types + 4 untouched)
        + "ascii_col ascii,"
        + "bigint_col bigint,"
        + "blob_col blob,"
        + "boolean_col boolean,"
        + "date_col date,"
        + "decimal_col decimal,"
        + "double_col double,"
        + "duration_col duration,"
        + "float_col float,"
        + "inet_col inet,"
        + "int_col int,"
        + "smallint_col smallint,"
        + "text_col text,"
        + "time_col time,"
        + "timestamp_col timestamp,"
        + "timeuuid_col timeuuid,"
        + "tinyint_col tinyint,"
        + "uuid_col uuid,"
        + "varchar_col varchar,"
        + "varint_col varint,"
        + "untouched_text text,"
        + "untouched_int int,"
        + "untouched_boolean boolean,"
        + "untouched_uuid uuid,"
        // Frozen collection columns (25 columns)
        + "frozen_list_col frozen<list<int>>,"
        + "frozen_set_col frozen<set<text>>,"
        + "frozen_map_col frozen<map<int, text>>,"
        + "frozen_tuple_col frozen<tuple<int, text>>,"
        + "frozen_map_ascii_col frozen<map<ascii, text>>,"
        + "frozen_map_bigint_col frozen<map<bigint, text>>,"
        + "frozen_map_boolean_col frozen<map<boolean, text>>,"
        + "frozen_map_date_col frozen<map<date, text>>,"
        + "frozen_map_decimal_col frozen<map<decimal, text>>,"
        + "frozen_map_double_col frozen<map<double, text>>,"
        + "frozen_map_float_col frozen<map<float, text>>,"
        + "frozen_map_int_col frozen<map<int, text>>,"
        + "frozen_map_inet_col frozen<map<inet, text>>,"
        + "frozen_map_smallint_col frozen<map<smallint, text>>,"
        + "frozen_map_text_col frozen<map<text, text>>,"
        + "frozen_map_time_col frozen<map<time, text>>,"
        + "frozen_map_timestamp_col frozen<map<timestamp, text>>,"
        + "frozen_map_timeuuid_col frozen<map<timeuuid, text>>,"
        + "frozen_map_tinyint_col frozen<map<tinyint, text>>,"
        + "frozen_map_uuid_col frozen<map<uuid, text>>,"
        + "frozen_map_varchar_col frozen<map<varchar, text>>,"
        + "frozen_map_varint_col frozen<map<varint, text>>,"
        + "frozen_map_tuple_key_col frozen<map<frozen<tuple<int, text>>, text>>,"
        + "frozen_map_udt_key_col frozen<map<frozen<map_key_udt>, text>>,"
        + "frozen_set_timeuuid_col frozen<set<timeuuid>>,"
        // Non-frozen collection columns (22 columns)
        + "list_col list<int>,"
        + "set_col set<text>,"
        + "map_col map<int, text>,"
        + "map_ascii_col map<ascii, text>,"
        + "map_bigint_col map<bigint, text>,"
        + "map_boolean_col map<boolean, text>,"
        + "map_date_col map<date, text>,"
        + "map_decimal_col map<decimal, text>,"
        + "map_double_col map<double, text>,"
        + "map_float_col map<float, text>,"
        + "map_int_col map<int, text>,"
        + "map_inet_col map<inet, text>,"
        + "map_smallint_col map<smallint, text>,"
        + "map_text_col map<text, text>,"
        + "map_time_col map<time, text>,"
        + "map_timestamp_col map<timestamp, text>,"
        + "map_timeuuid_col map<timeuuid, text>,"
        + "map_tinyint_col map<tinyint, text>,"
        + "map_uuid_col map<uuid, text>,"
        + "map_varchar_col map<varchar, text>,"
        + "map_varint_col map<varint, text>,"
        + "set_timeuuid_col set<timeuuid>,"
        // UDT columns (10 columns)
        + "frozen_udt frozen<simple_udt>,"
        + "nf_udt simple_udt,"
        + "frozen_nested_udt frozen<nested_udt>,"
        + "nf_nested_udt nested_udt,"
        + "frozen_udt_with_map frozen<udt_with_map>,"
        + "nf_udt_with_map udt_with_map,"
        + "frozen_udt_with_list frozen<udt_with_list>,"
        + "nf_udt_with_list udt_with_list,"
        + "frozen_udt_with_set frozen<udt_with_set>,"
        + "nf_udt_with_set udt_with_set"
        + ")";
  }

  // ===================== Primitive CQL Helper Methods =====================

  private static final String PRIMITIVE_ALL_COLUMNS =
      "id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, "
          + "duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, "
          + "timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, "
          + "untouched_text, untouched_int, untouched_boolean, untouched_uuid";

  private static final String PRIMITIVE_BASE_VALUES_SET1 =
      "'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, "
          + "2.71828, '127.0.0.1', 42, 7, '%s', '12:34:56.789', '2024-06-10T12:34:56.789Z', "
          + "81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, "
          + "'varchar text', 999999999";

  private static final String PRIMITIVE_EMPTY_VALUES_SET1 =
      "'', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, "
          + "2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', "
          + "81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, "
          + "'', 999999999";

  private static final String PRIMITIVE_SET_ALL_NULL =
      "ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, "
          + "date_col = null, decimal_col = null, double_col = null, duration_col = null, "
          + "float_col = null, inet_col = null, int_col = null, smallint_col = null, "
          + "text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, "
          + "tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null";

  private static final String PRIMITIVE_SET_VALUES_SET1 =
      "ascii_col = 'ascii', bigint_col = 1234567890123, blob_col = 0xCAFEBABE, "
          + "boolean_col = true, date_col = '2024-06-10', decimal_col = 12345.67, "
          + "double_col = 3.14159, duration_col = 1d12h30m, float_col = 2.71828, "
          + "inet_col = '127.0.0.1', int_col = 42, smallint_col = 7, text_col = 'value', "
          + "time_col = '12:34:56.789', timestamp_col = '2024-06-10T12:34:56.789Z', "
          + "timeuuid_col = 81d4a030-4632-11f0-9484-409dd8f36eba, tinyint_col = 5, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371c, varchar_col = 'varchar text', "
          + "varint_col = 999999999";

  private static final String PRIMITIVE_SET_VALUES_SET2 =
      "ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, "
          + "boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, "
          + "double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, "
          + "inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value2', "
          + "time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', "
          + "timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', "
          + "varint_col = 888888888";

  private static final String PRIMITIVE_SET_EMPTY_SET2 =
      "ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, "
          + "boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, "
          + "double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, "
          + "inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', "
          + "time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', "
          + "timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', "
          + "varint_col = 888888888";

  protected void primitiveInsertFullRow(int pk, String textColValue) {
    String values = PRIMITIVE_BASE_VALUES_SET1.formatted(textColValue);
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s, '%s', %d, %s, %s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                PRIMITIVE_ALL_COLUMNS,
                pk,
                values,
                UNTOUCHED_TEXT_VALUE,
                UNTOUCHED_INT_VALUE,
                UNTOUCHED_BOOLEAN_VALUE,
                UNTOUCHED_UUID_VALUE));
  }

  protected void primitiveInsertEmptyRow(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s, '%s', %d, %s, %s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                PRIMITIVE_ALL_COLUMNS,
                pk,
                PRIMITIVE_EMPTY_VALUES_SET1,
                UNTOUCHED_TEXT_VALUE,
                UNTOUCHED_INT_VALUE,
                UNTOUCHED_BOOLEAN_VALUE,
                UNTOUCHED_UUID_VALUE));
  }

  protected void primitiveInsertOnlyUntouchedRow(int pk) {
    session.execute(
        ("INSERT INTO %s (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) "
                + "VALUES (%d, '%s', %d, %s, %s)")
            .formatted(
                getSuiteKeyspaceTableName(),
                pk,
                UNTOUCHED_TEXT_VALUE,
                UNTOUCHED_INT_VALUE,
                UNTOUCHED_BOOLEAN_VALUE,
                UNTOUCHED_UUID_VALUE));
  }

  protected void primitiveUpdateToNull(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), PRIMITIVE_SET_ALL_NULL, pk));
  }

  protected void primitiveUpdateToValuesSet1(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), PRIMITIVE_SET_VALUES_SET1, pk));
  }

  protected void primitiveUpdateToValuesSet2(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), PRIMITIVE_SET_VALUES_SET2, pk));
  }

  protected void primitiveUpdateToEmptySet2(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), PRIMITIVE_SET_EMPTY_SET2, pk));
  }

  protected void primitiveDeleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  // ===================== Frozen Collections CQL Helper Methods =====================

  private static final String FROZEN_INSERT_COLUMNS =
      "id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col, "
          + "frozen_map_ascii_col, frozen_map_bigint_col, frozen_map_boolean_col, "
          + "frozen_map_date_col, frozen_map_decimal_col, frozen_map_double_col, "
          + "frozen_map_float_col, frozen_map_int_col, frozen_map_inet_col, "
          + "frozen_map_smallint_col, frozen_map_text_col, frozen_map_time_col, "
          + "frozen_map_timestamp_col, frozen_map_timeuuid_col, frozen_map_tinyint_col, "
          + "frozen_map_uuid_col, frozen_map_varchar_col, frozen_map_varint_col, "
          + "frozen_map_tuple_key_col, frozen_map_udt_key_col, frozen_set_timeuuid_col";

  private static String frozenValuesWithValues(int pk) {
    return pk
        + ", [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo'), "
        + "{'ascii_key': 'ascii_value'}, {1234567890123: 'bigint_value'}, "
        + "{true: 'boolean_value'}, {'2024-06-10': 'date_value'}, "
        + "{12345.67: 'decimal_value'}, {3.14159: 'double_value'}, "
        + "{2.71828: 'float_value'}, {42: 'int_value'}, "
        + "{'127.0.0.1': 'inet_value'}, {7: 'smallint_value'}, "
        + "{'text_key': 'text_value'}, {'12:34:56.789': 'time_value'}, "
        + "{'2024-06-10T12:34:56.789Z': 'timestamp_value'}, "
        + "{81d4a030-4632-11f0-9484-409dd8f36eba: 'timeuuid_value'}, "
        + "{5: 'tinyint_value'}, {453662fa-db4b-4938-9033-d8523c0a371c: 'uuid_value'}, "
        + "{'varchar_key': 'varchar_value'}, {999999999: 'varint_value'}, "
        + "{(1, 'tuple_key'): 'tuple_value'}, {{a: 1, b: 'udt_key'}: 'udt_value'}, "
        + "{81d4a030-4632-11f0-9484-409dd8f36eba}";
  }

  private static String frozenValuesWithEmpty(int pk) {
    return pk
        + ", [], {}, {}, (null, null), "
        + "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}";
  }

  private static String frozenValuesWithNull(int pk) {
    return pk
        + ", null, null, null, null, "
        + "null, null, null, null, null, null, null, null, null, null, null, null, "
        + "null, null, null, null, null, null, null, null, null";
  }

  private static final String FROZEN_UPDATE_SET_VALUES =
      "frozen_list_col = [4, 5, 6], frozen_set_col = {'x', 'y', 'z'}, "
          + "frozen_map_col = {3: 'three', 4: 'four'}, frozen_tuple_col = (99, 'bar'), "
          + "frozen_map_ascii_col = {'ascii_key_2': 'ascii_value_2'}, "
          + "frozen_map_bigint_col = {1234567890124: 'bigint_value_2'}, "
          + "frozen_map_boolean_col = {false: 'boolean_value_2'}, "
          + "frozen_map_date_col = {'2024-06-11': 'date_value_2'}, "
          + "frozen_map_decimal_col = {98765.43: 'decimal_value_2'}, "
          + "frozen_map_double_col = {2.71828: 'double_value_2'}, "
          + "frozen_map_float_col = {1.41421: 'float_value_2'}, "
          + "frozen_map_int_col = {43: 'int_value_2'}, "
          + "frozen_map_inet_col = {'127.0.0.2': 'inet_value_2'}, "
          + "frozen_map_smallint_col = {8: 'smallint_value_2'}, "
          + "frozen_map_text_col = {'text_key_2': 'text_value_2'}, "
          + "frozen_map_time_col = {'01:02:03.456': 'time_value_2'}, "
          + "frozen_map_timestamp_col = {'2024-06-11T01:02:03.456Z': 'timestamp_value_2'}, "
          + "frozen_map_timeuuid_col = {81d4a031-4632-11f0-9484-409dd8f36eba: 'timeuuid_value_2'}, "
          + "frozen_map_tinyint_col = {6: 'tinyint_value_2'}, "
          + "frozen_map_uuid_col = {453662fa-db4b-4938-9033-d8523c0a371d: 'uuid_value_2'}, "
          + "frozen_map_varchar_col = {'varchar_key_2': 'varchar_value_2'}, "
          + "frozen_map_varint_col = {888888888: 'varint_value_2'}, "
          + "frozen_map_tuple_key_col = {(2, 'tuple_key_2'): 'tuple_value_2'}, "
          + "frozen_map_udt_key_col = {{a: 2, b: 'udt_key_2'}: 'udt_value_2'}, "
          + "frozen_set_timeuuid_col = {81d4a031-4632-11f0-9484-409dd8f36eba}";

  private static final String FROZEN_UPDATE_SET_INITIAL_VALUES =
      "frozen_list_col = [1, 2, 3], frozen_set_col = {'a', 'b', 'c'}, "
          + "frozen_map_col = {1: 'one', 2: 'two'}, frozen_tuple_col = (42, 'foo'), "
          + "frozen_map_ascii_col = {'ascii_key': 'ascii_value'}, "
          + "frozen_map_bigint_col = {1234567890123: 'bigint_value'}, "
          + "frozen_map_boolean_col = {true: 'boolean_value'}, "
          + "frozen_map_date_col = {'2024-06-10': 'date_value'}, "
          + "frozen_map_decimal_col = {12345.67: 'decimal_value'}, "
          + "frozen_map_double_col = {3.14159: 'double_value'}, "
          + "frozen_map_float_col = {2.71828: 'float_value'}, "
          + "frozen_map_int_col = {42: 'int_value'}, "
          + "frozen_map_inet_col = {'127.0.0.1': 'inet_value'}, "
          + "frozen_map_smallint_col = {7: 'smallint_value'}, "
          + "frozen_map_text_col = {'text_key': 'text_value'}, "
          + "frozen_map_time_col = {'12:34:56.789': 'time_value'}, "
          + "frozen_map_timestamp_col = {'2024-06-10T12:34:56.789Z': 'timestamp_value'}, "
          + "frozen_map_timeuuid_col = {81d4a030-4632-11f0-9484-409dd8f36eba: 'timeuuid_value'}, "
          + "frozen_map_tinyint_col = {5: 'tinyint_value'}, "
          + "frozen_map_uuid_col = {453662fa-db4b-4938-9033-d8523c0a371c: 'uuid_value'}, "
          + "frozen_map_varchar_col = {'varchar_key': 'varchar_value'}, "
          + "frozen_map_varint_col = {999999999: 'varint_value'}, "
          + "frozen_map_tuple_key_col = {(1, 'tuple_key'): 'tuple_value'}, "
          + "frozen_map_udt_key_col = {{a: 1, b: 'udt_key'}: 'udt_value'}, "
          + "frozen_set_timeuuid_col = {81d4a030-4632-11f0-9484-409dd8f36eba}";

  private static final String FROZEN_UPDATE_SET_EMPTY =
      "frozen_list_col = [], frozen_set_col = {}, "
          + "frozen_map_col = {}, frozen_tuple_col = (null, null), "
          + "frozen_map_ascii_col = {}, frozen_map_bigint_col = {}, "
          + "frozen_map_boolean_col = {}, frozen_map_date_col = {}, "
          + "frozen_map_decimal_col = {}, frozen_map_double_col = {}, "
          + "frozen_map_float_col = {}, frozen_map_int_col = {}, "
          + "frozen_map_inet_col = {}, frozen_map_smallint_col = {}, "
          + "frozen_map_text_col = {}, frozen_map_time_col = {}, "
          + "frozen_map_timestamp_col = {}, frozen_map_timeuuid_col = {}, "
          + "frozen_map_tinyint_col = {}, frozen_map_uuid_col = {}, "
          + "frozen_map_varchar_col = {}, frozen_map_varint_col = {}, "
          + "frozen_map_tuple_key_col = {}, frozen_map_udt_key_col = {}, "
          + "frozen_set_timeuuid_col = {}";

  private static final String FROZEN_UPDATE_SET_NULL =
      "frozen_list_col = null, frozen_set_col = null, "
          + "frozen_map_col = null, frozen_tuple_col = null, "
          + "frozen_map_ascii_col = null, frozen_map_bigint_col = null, "
          + "frozen_map_boolean_col = null, frozen_map_date_col = null, "
          + "frozen_map_decimal_col = null, frozen_map_double_col = null, "
          + "frozen_map_float_col = null, frozen_map_int_col = null, "
          + "frozen_map_inet_col = null, frozen_map_smallint_col = null, "
          + "frozen_map_text_col = null, frozen_map_time_col = null, "
          + "frozen_map_timestamp_col = null, frozen_map_timeuuid_col = null, "
          + "frozen_map_tinyint_col = null, frozen_map_uuid_col = null, "
          + "frozen_map_varchar_col = null, frozen_map_varint_col = null, "
          + "frozen_map_tuple_key_col = null, frozen_map_udt_key_col = null, "
          + "frozen_set_timeuuid_col = null";

  protected void frozenInsertWithValues(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(), FROZEN_INSERT_COLUMNS, frozenValuesWithValues(pk)));
  }

  protected void frozenInsertWithEmpty(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(), FROZEN_INSERT_COLUMNS, frozenValuesWithEmpty(pk)));
  }

  protected void frozenInsertWithNull(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(), FROZEN_INSERT_COLUMNS, frozenValuesWithNull(pk)));
  }

  protected void frozenInsertOnlyPk(int pk) {
    session.execute("INSERT INTO %s (id) VALUES (%d)".formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void frozenUpdateToValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), FROZEN_UPDATE_SET_VALUES, pk));
  }

  protected void frozenUpdateToInitialValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), FROZEN_UPDATE_SET_INITIAL_VALUES, pk));
  }

  protected void frozenUpdateToEmpty(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), FROZEN_UPDATE_SET_EMPTY, pk));
  }

  protected void frozenUpdateToNull(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), FROZEN_UPDATE_SET_NULL, pk));
  }

  protected void frozenDeleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  // ===================== Non-Frozen Collections CQL Helper Methods =====================

  private static final String NONFROZEN_INSERT_COLUMNS =
      "id, list_col, set_col, map_col, "
          + "map_ascii_col, map_bigint_col, map_boolean_col, "
          + "map_date_col, map_decimal_col, map_double_col, "
          + "map_float_col, map_int_col, map_inet_col, "
          + "map_smallint_col, map_text_col, map_time_col, "
          + "map_timestamp_col, map_timeuuid_col, map_tinyint_col, "
          + "map_uuid_col, map_varchar_col, map_varint_col, "
          + "set_timeuuid_col";

  private static String nonFrozenValuesWithValues(int pk) {
    return pk
        + ", [10, 20, 30], {'x', 'y', 'z'}, {10: 'ten', 20: 'twenty'}, "
        + "{'ascii_key': 'ascii_value'}, {1234567890123: 'bigint_value'}, "
        + "{true: 'boolean_value'}, {'2024-06-10': 'date_value'}, "
        + "{12345.67: 'decimal_value'}, {3.14159: 'double_value'}, "
        + "{2.71828: 'float_value'}, {42: 'int_value'}, "
        + "{'127.0.0.1': 'inet_value'}, {7: 'smallint_value'}, "
        + "{'text_key': 'text_value'}, {'12:34:56.789': 'time_value'}, "
        + "{'2024-06-10T12:34:56.789Z': 'timestamp_value'}, "
        + "{81d4a030-4632-11f0-9484-409dd8f36eba: 'timeuuid_value'}, "
        + "{5: 'tinyint_value'}, {453662fa-db4b-4938-9033-d8523c0a371c: 'uuid_value'}, "
        + "{'varchar_key': 'varchar_value'}, {999999999: 'varint_value'}, "
        + "{81d4a030-4632-11f0-9484-409dd8f36eba}";
  }

  private static String nonFrozenValuesWithNull(int pk) {
    return pk
        + ", null, null, null, "
        + "null, null, null, null, null, null, null, null, null, null, null, null, "
        + "null, null, null, null, null, null, null";
  }

  private static String nonFrozenValuesWithEmpty(int pk) {
    return pk
        + ", [], {}, {}, "
        + "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "
        + "{}, {}, {}, {}, {}, {}, {}";
  }

  private static final String NONFROZEN_UPDATE_SET_VALUES =
      "list_col = [40, 50, 60], set_col = {'a', 'b', 'c'}, "
          + "map_col = {30: 'thirty', 40: 'forty'}, "
          + "map_ascii_col = {'ascii_key_2': 'ascii_value_2'}, "
          + "map_bigint_col = {1234567890124: 'bigint_value_2'}, "
          + "map_boolean_col = {false: 'boolean_value_2'}, "
          + "map_date_col = {'2024-06-11': 'date_value_2'}, "
          + "map_decimal_col = {98765.43: 'decimal_value_2'}, "
          + "map_double_col = {2.71828: 'double_value_2'}, "
          + "map_float_col = {1.41421: 'float_value_2'}, "
          + "map_int_col = {43: 'int_value_2'}, "
          + "map_inet_col = {'127.0.0.2': 'inet_value_2'}, "
          + "map_smallint_col = {8: 'smallint_value_2'}, "
          + "map_text_col = {'text_key_2': 'text_value_2'}, "
          + "map_time_col = {'01:02:03.456': 'time_value_2'}, "
          + "map_timestamp_col = {'2024-06-11T01:02:03.456Z': 'timestamp_value_2'}, "
          + "map_timeuuid_col = {81d4a031-4632-11f0-9484-409dd8f36eba: 'timeuuid_value_2'}, "
          + "map_tinyint_col = {6: 'tinyint_value_2'}, "
          + "map_uuid_col = {453662fa-db4b-4938-9033-d8523c0a371d: 'uuid_value_2'}, "
          + "map_varchar_col = {'varchar_key_2': 'varchar_value_2'}, "
          + "map_varint_col = {888888888: 'varint_value_2'}, "
          + "set_timeuuid_col = {81d4a031-4632-11f0-9484-409dd8f36eba}";

  private static final String NONFROZEN_UPDATE_SET_INITIAL_VALUES =
      "list_col = [10, 20, 30], set_col = {'x', 'y', 'z'}, "
          + "map_col = {10: 'ten', 20: 'twenty'}, "
          + "map_ascii_col = {'ascii_key': 'ascii_value'}, "
          + "map_bigint_col = {1234567890123: 'bigint_value'}, "
          + "map_boolean_col = {true: 'boolean_value'}, "
          + "map_date_col = {'2024-06-10': 'date_value'}, "
          + "map_decimal_col = {12345.67: 'decimal_value'}, "
          + "map_double_col = {3.14159: 'double_value'}, "
          + "map_float_col = {2.71828: 'float_value'}, "
          + "map_int_col = {42: 'int_value'}, "
          + "map_inet_col = {'127.0.0.1': 'inet_value'}, "
          + "map_smallint_col = {7: 'smallint_value'}, "
          + "map_text_col = {'text_key': 'text_value'}, "
          + "map_time_col = {'12:34:56.789': 'time_value'}, "
          + "map_timestamp_col = {'2024-06-10T12:34:56.789Z': 'timestamp_value'}, "
          + "map_timeuuid_col = {81d4a030-4632-11f0-9484-409dd8f36eba: 'timeuuid_value'}, "
          + "map_tinyint_col = {5: 'tinyint_value'}, "
          + "map_uuid_col = {453662fa-db4b-4938-9033-d8523c0a371c: 'uuid_value'}, "
          + "map_varchar_col = {'varchar_key': 'varchar_value'}, "
          + "map_varint_col = {999999999: 'varint_value'}, "
          + "set_timeuuid_col = {81d4a030-4632-11f0-9484-409dd8f36eba}";

  private static final String NONFROZEN_UPDATE_SET_EMPTY =
      "list_col = [], set_col = {}, "
          + "map_col = {}, "
          + "map_ascii_col = {}, map_bigint_col = {}, "
          + "map_boolean_col = {}, map_date_col = {}, "
          + "map_decimal_col = {}, map_double_col = {}, "
          + "map_float_col = {}, map_int_col = {}, "
          + "map_inet_col = {}, map_smallint_col = {}, "
          + "map_text_col = {}, map_time_col = {}, "
          + "map_timestamp_col = {}, map_timeuuid_col = {}, "
          + "map_tinyint_col = {}, map_uuid_col = {}, "
          + "map_varchar_col = {}, map_varint_col = {}, "
          + "set_timeuuid_col = {}";

  private static final String NONFROZEN_UPDATE_SET_NULL =
      "list_col = null, set_col = null, "
          + "map_col = null, "
          + "map_ascii_col = null, map_bigint_col = null, "
          + "map_boolean_col = null, map_date_col = null, "
          + "map_decimal_col = null, map_double_col = null, "
          + "map_float_col = null, map_int_col = null, "
          + "map_inet_col = null, map_smallint_col = null, "
          + "map_text_col = null, map_time_col = null, "
          + "map_timestamp_col = null, map_timeuuid_col = null, "
          + "map_tinyint_col = null, map_uuid_col = null, "
          + "map_varchar_col = null, map_varint_col = null, "
          + "set_timeuuid_col = null";

  protected void nonFrozenInsertWithValues(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                NONFROZEN_INSERT_COLUMNS,
                nonFrozenValuesWithValues(pk)));
  }

  protected void nonFrozenInsertWithNull(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                NONFROZEN_INSERT_COLUMNS,
                nonFrozenValuesWithNull(pk)));
  }

  protected void nonFrozenInsertWithEmpty(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                NONFROZEN_INSERT_COLUMNS,
                nonFrozenValuesWithEmpty(pk)));
  }

  protected void nonFrozenInsertMinimal(int pk) {
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, [10], {'x'}, {10: 'ten'})"
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void nonFrozenInsertListOnly(int pk, String listValue) {
    session.execute(
        "INSERT INTO %s (id, list_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, listValue));
  }

  protected void nonFrozenInsertSetOnly(int pk, String setValue) {
    session.execute(
        "INSERT INTO %s (id, set_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, setValue));
  }

  protected void nonFrozenInsertMapOnly(int pk, String mapValue) {
    session.execute(
        "INSERT INTO %s (id, map_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, mapValue));
  }

  protected void nonFrozenDeleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void nonFrozenUpdateListAppend(int pk, String elements) {
    session.execute(
        "UPDATE %s SET list_col = list_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  protected void nonFrozenUpdateListRemove(int pk, String elements) {
    session.execute(
        "UPDATE %s SET list_col = list_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  protected void nonFrozenUpdateSetAdd(int pk, String elements) {
    session.execute(
        "UPDATE %s SET set_col = set_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  protected void nonFrozenUpdateSetRemove(int pk, String elements) {
    session.execute(
        "UPDATE %s SET set_col = set_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  protected void nonFrozenUpdateMapAdd(int pk, String entries) {
    session.execute(
        "UPDATE %s SET map_col = map_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), entries, pk));
  }

  protected void nonFrozenUpdateMapRemove(int pk, String keys) {
    session.execute(
        "UPDATE %s SET map_col = map_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), keys, pk));
  }

  protected void nonFrozenUpdateToValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), NONFROZEN_UPDATE_SET_VALUES, pk));
  }

  protected void nonFrozenUpdateToInitialValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), NONFROZEN_UPDATE_SET_INITIAL_VALUES, pk));
  }

  protected void nonFrozenUpdateToNull(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), NONFROZEN_UPDATE_SET_NULL, pk));
  }

  protected void nonFrozenUpdateToEmpty(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), NONFROZEN_UPDATE_SET_EMPTY, pk));
  }

  // ===================== UDT CQL Helper Methods =====================

  private static final String UDT_ALL_COLUMNS =
      "id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
          + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
          + "frozen_udt_with_set, nf_udt_with_set";

  private static final String UDT_VALUES_FULL =
      "{a: 42, b: 'foo'}, "
          + "{a: 7, b: 'bar'}, "
          + "{inner: {x: 10, y: 'hello'}, z: 20}, "
          + "{inner: {x: 10, y: 'hello'}, z: 20}, "
          + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
          + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
          + "{l: [1, 2, 3]}, "
          + "{l: [1, 2, 3]}, "
          + "{s: {'a', 'b', 'c'}}, "
          + "{s: {'a', 'b', 'c'}}";

  private static final String UDT_VALUES_NULL =
      "null, null, null, null, null, null, null, null, null, null";

  private static final String UDT_VALUES_EMPTY =
      "{a: null, b: null}, "
          + "{a: null, b: null}, "
          + "{inner: null, z: null}, "
          + "{inner: null, z: null}, "
          + "{m: null}, "
          + "{m: null}, "
          + "{l: null}, "
          + "{l: null}, "
          + "{s: null}, "
          + "{s: null}";

  protected void udtInsertFull(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk, UDT_VALUES_FULL));
  }

  protected void udtInsertNull(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk, UDT_VALUES_NULL));
  }

  protected void udtInsertEmpty(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk, UDT_VALUES_EMPTY));
  }

  protected void udtInsertForFrozenUpdate(int pk) {
    // Insert with nf_udt having same value as frozen_udt for this specific test
    session.execute(
        ("INSERT INTO %s (%s) VALUES (%d, "
                + "{a: 42, b: 'foo'}, "
                + "{a: 42, b: 'foo'}, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{l: [1, 2, 3]}, "
                + "{l: [1, 2, 3]}, "
                + "{s: {'a', 'b', 'c'}}, "
                + "{s: {'a', 'b', 'c'}}"
                + ")")
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk));
  }

  protected void udtInsertForValueToNull(int pk) {
    // Insert with nf_udt null for this specific test
    session.execute(
        ("INSERT INTO %s (%s) VALUES (%d, "
                + "{a: 42, b: 'foo'}, "
                + "null, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{l: [1, 2, 3]}, "
                + "{l: [1, 2, 3]}, "
                + "{s: {'a', 'b', 'c'}}, "
                + "{s: {'a', 'b', 'c'}}"
                + ")")
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk));
  }

  protected void udtInsertForNonFrozenField(int pk) {
    // Insert with frozen_udt null for this specific test
    session.execute(
        ("INSERT INTO %s (%s) VALUES (%d, "
                + "null, "
                + "{a: 7, b: 'bar'}, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{inner: {x: 10, y: 'hello'}, z: 20}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "{l: [1, 2, 3]}, "
                + "{l: [1, 2, 3]}, "
                + "{s: {'a', 'b', 'c'}}, "
                + "{s: {'a', 'b', 'c'}}"
                + ")")
            .formatted(getSuiteKeyspaceTableName(), UDT_ALL_COLUMNS, pk));
  }

  protected void udtDeleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateFrozenToValue(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "frozen_udt = {a: 99, b: 'updated'}, "
                + "nf_udt.a = 77, "
                + "frozen_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
                + "nf_nested_udt.z = 21, "
                + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}}, "
                + "nf_udt_with_map.m = {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}, "
                + "frozen_udt_with_list = {l: [4, 5, 6]}, "
                + "nf_udt_with_list.l = [4, 5, 6], "
                + "frozen_udt_with_set = {s: {'d', 'e'}}, "
                + "nf_udt_with_set.s = {'d', 'e'} "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateAllToNull(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "frozen_udt = null, "
                + "nf_udt = null, "
                + "frozen_nested_udt = null, "
                + "nf_nested_udt = null, "
                + "frozen_udt_with_map = null, "
                + "nf_udt_with_map = null, "
                + "frozen_udt_with_list = null, "
                + "nf_udt_with_list = null, "
                + "frozen_udt_with_set = null, "
                + "nf_udt_with_set = null "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateAllToEmpty(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "frozen_udt = {a: null, b: null}, "
                + "nf_udt = {a: null, b: null}, "
                + "frozen_nested_udt = {inner: null, z: null}, "
                + "nf_nested_udt = {inner: null, z: null}, "
                + "frozen_udt_with_map = {m: null}, "
                + "nf_udt_with_map = {m: null}, "
                + "frozen_udt_with_list = {l: null}, "
                + "nf_udt_with_list = {l: null}, "
                + "frozen_udt_with_set = {s: null}, "
                + "nf_udt_with_set = {s: null} "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateFromNullToValue(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "frozen_udt = {a: 42, b: 'foo'}, "
                + "nf_udt = {a: 7, b: 'bar'}, "
                + "frozen_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
                + "nf_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
                + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "nf_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
                + "frozen_udt_with_list = {l: [1, 2, 3]}, "
                + "nf_udt_with_list = {l: [1, 2, 3]}, "
                + "frozen_udt_with_set = {s: {'a', 'b', 'c'}}, "
                + "nf_udt_with_set = {s: {'a', 'b', 'c'}} "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateNonFrozenField(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "nf_udt.a = 100, "
                + "frozen_udt = {a: 99, b: 'updated'}, "
                + "frozen_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
                + "nf_nested_udt.z = 21, "
                + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}}, "
                + "nf_udt_with_map.m = {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}, "
                + "frozen_udt_with_list = {l: [4, 5, 6]}, "
                + "nf_udt_with_list.l = [4, 5, 6], "
                + "frozen_udt_with_set = {s: {'d', 'e'}}, "
                + "nf_udt_with_set.s = {'d', 'e'} "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateNonFrozenReplace(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "nf_udt = {a: 99, b: 'updated'}, "
                + "nf_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
                + "nf_udt_with_map = {m: {81d4a032-4632-11f0-9484-409dd8f36eba: 'value3', 81d4a033-4632-11f0-9484-409dd8f36eba: 'value4'}}, "
                + "nf_udt_with_list = {l: [4, 5, 6]}, "
                + "nf_udt_with_set = {s: {'d', 'e', 'f'}} "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  protected void udtUpdateNonFrozenToNull(int pk) {
    session.execute(
        ("UPDATE %s SET "
                + "nf_udt = null, "
                + "nf_nested_udt = null, "
                + "nf_udt_with_map = null, "
                + "nf_udt_with_list = null, "
                + "nf_udt_with_set = null "
                + "WHERE id = %d")
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  // ===================== Primitive Test Methods =====================

  @Test
  void testPrimitiveInsert() {
    int pk = reservePk();
    primitiveInsertFullRow(pk, "some text");
    waitAndAssert(pk, expectedPrimitiveInsert(pk));
  }

  @Test
  void testPrimitiveDelete() {
    int pk = reservePk();
    primitiveInsertFullRow(pk, "delete me");
    primitiveDeleteRow(pk);
    waitAndAssert(pk, expectedPrimitiveDelete(pk));
  }

  @Test
  void testPrimitiveUpdateFromValueToNil() {
    int pk = reservePk();
    primitiveInsertFullRow(pk, "value");
    primitiveUpdateToNull(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromValueToNil(pk));
  }

  @Test
  void testPrimitiveUpdateFromValueToEmpty() {
    int pk = reservePk();
    primitiveInsertFullRow(pk, "value");
    primitiveUpdateToEmptySet2(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromValueToEmpty(pk));
  }

  @Test
  void testPrimitiveUpdateFromValueToValue() {
    int pk = reservePk();
    primitiveInsertFullRow(pk, "value");
    primitiveUpdateToValuesSet2(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromValueToValue(pk));
  }

  @Test
  void testPrimitiveUpdateFromNilToValue() {
    int pk = reservePk();
    primitiveInsertOnlyUntouchedRow(pk);
    primitiveUpdateToValuesSet1(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromNilToValue(pk));
  }

  @Test
  void testPrimitiveUpdateFromNilToEmpty() {
    int pk = reservePk();
    primitiveInsertOnlyUntouchedRow(pk);
    primitiveUpdateToEmptySet2(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromNilToEmpty(pk));
  }

  @Test
  void testPrimitiveUpdateFromNilToNil() {
    int pk = reservePk();
    primitiveInsertOnlyUntouchedRow(pk);
    primitiveUpdateToNull(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromNilToNil(pk));
  }

  @Test
  void testPrimitiveUpdateFromEmptyToValue() {
    int pk = reservePk();
    primitiveInsertEmptyRow(pk);
    primitiveUpdateToValuesSet2(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromEmptyToValue(pk));
  }

  @Test
  void testPrimitiveUpdateFromEmptyToNil() {
    int pk = reservePk();
    primitiveInsertEmptyRow(pk);
    primitiveUpdateToNull(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromEmptyToNil(pk));
  }

  @Test
  void testPrimitiveUpdateFromEmptyToEmpty() {
    int pk = reservePk();
    primitiveInsertEmptyRow(pk);
    primitiveUpdateToEmptySet2(pk);
    waitAndAssert(pk, expectedPrimitiveUpdateFromEmptyToEmpty(pk));
  }

  // ===================== Frozen Collections Test Methods =====================

  @Test
  void testFrozenCollectionsInsertWithValues() {
    int pk = reservePk();
    frozenInsertWithValues(pk);
    waitAndAssert(pk, expectedFrozenCollectionsInsertWithValues(pk));
  }

  @Test
  void testFrozenCollectionsInsertWithEmpty() {
    int pk = reservePk();
    frozenInsertWithEmpty(pk);
    waitAndAssert(pk, expectedFrozenCollectionsInsertWithEmpty(pk));
  }

  @Test
  void testFrozenCollectionsInsertWithNull() {
    int pk = reservePk();
    frozenInsertWithNull(pk);
    waitAndAssert(pk, expectedFrozenCollectionsInsertWithNull(pk));
  }

  @Test
  void testFrozenCollectionsDelete() {
    int pk = reservePk();
    frozenInsertWithValues(pk);
    frozenDeleteRow(pk);
    waitAndAssert(pk, expectedFrozenCollectionsDelete(pk));
  }

  @Test
  void testFrozenCollectionsUpdateFromValueToValue() {
    int pk = reservePk();
    frozenInsertWithValues(pk);
    frozenUpdateToValues(pk);
    waitAndAssert(pk, expectedFrozenCollectionsUpdateFromValueToValue(pk));
  }

  @Test
  void testFrozenCollectionsUpdateFromValueToEmpty() {
    int pk = reservePk();
    frozenInsertWithValues(pk);
    frozenUpdateToEmpty(pk);
    waitAndAssert(pk, expectedFrozenCollectionsUpdateFromValueToEmpty(pk));
  }

  @Test
  void testFrozenCollectionsUpdateFromValueToNull() {
    int pk = reservePk();
    frozenInsertWithValues(pk);
    frozenUpdateToNull(pk);
    waitAndAssert(pk, expectedFrozenCollectionsUpdateFromValueToNull(pk));
  }

  @Test
  void testFrozenCollectionsUpdateFromEmptyToValue() {
    int pk = reservePk();
    frozenInsertWithEmpty(pk);
    frozenUpdateToInitialValues(pk);
    waitAndAssert(pk, expectedFrozenCollectionsUpdateFromEmptyToValue(pk));
  }

  @Test
  void testFrozenCollectionsUpdateFromNullToValue() {
    int pk = reservePk();
    frozenInsertOnlyPk(pk);
    frozenUpdateToInitialValues(pk);
    waitAndAssert(pk, expectedFrozenCollectionsUpdateFromNullToValue(pk));
  }

  // ===================== Non-Frozen Collections Test Methods =====================

  @Test
  void testNonFrozenCollectionsInsertWithValues() {
    int pk = reservePk();
    nonFrozenInsertWithValues(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsInsertWithValues(pk));
  }

  @Test
  void testNonFrozenCollectionsInsertWithNull() {
    int pk = reservePk();
    nonFrozenInsertWithNull(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsInsertWithNull(pk));
  }

  @Test
  void testNonFrozenCollectionsDelete() {
    int pk = reservePk();
    nonFrozenInsertMinimal(pk);
    nonFrozenDeleteRow(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsDelete(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateListAddElement() {
    int pk = reservePk();
    nonFrozenInsertListOnly(pk, "[10, 20]");
    nonFrozenUpdateListAppend(pk, "[30]");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateListAddElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateSetAddElement() {
    int pk = reservePk();
    nonFrozenInsertSetOnly(pk, "{'x', 'y'}");
    nonFrozenUpdateSetAdd(pk, "{'z'}");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateSetAddElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateMapAddElement() {
    int pk = reservePk();
    nonFrozenInsertMapOnly(pk, "{10: 'ten'}");
    nonFrozenUpdateMapAdd(pk, "{20: 'twenty'}");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateMapAddElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateListRemoveElement() {
    int pk = reservePk();
    nonFrozenInsertListOnly(pk, "[10, 20, 30]");
    nonFrozenUpdateListRemove(pk, "[20]");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateListRemoveElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateSetRemoveElement() {
    int pk = reservePk();
    nonFrozenInsertSetOnly(pk, "{'x', 'y', 'z'}");
    nonFrozenUpdateSetRemove(pk, "{'y'}");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateSetRemoveElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateMapRemoveElement() {
    int pk = reservePk();
    nonFrozenInsertMapOnly(pk, "{10: 'ten', 20: 'twenty'}");
    nonFrozenUpdateMapRemove(pk, "{10}");
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateMapRemoveElement(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateFromValueToValue() {
    int pk = reservePk();
    nonFrozenInsertWithValues(pk);
    nonFrozenUpdateToValues(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateFromValueToValue(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateFromValueToNull() {
    int pk = reservePk();
    nonFrozenInsertWithValues(pk);
    nonFrozenUpdateToNull(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateFromValueToNull(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateFromValueToEmpty() {
    int pk = reservePk();
    nonFrozenInsertWithValues(pk);
    nonFrozenUpdateToEmpty(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateFromValueToEmpty(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateFromNullToValue() {
    int pk = reservePk();
    nonFrozenInsertWithNull(pk);
    nonFrozenUpdateToInitialValues(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateFromNullToValue(pk));
  }

  @Test
  void testNonFrozenCollectionsUpdateFromEmptyToValue() {
    int pk = reservePk();
    nonFrozenInsertWithEmpty(pk);
    nonFrozenUpdateToInitialValues(pk);
    waitAndAssert(pk, expectedNonFrozenCollectionsUpdateFromEmptyToValue(pk));
  }

  // ===================== UDT Test Methods =====================

  @Test
  void testUdtInsert() {
    int pk = reservePk();
    udtInsertFull(pk);
    waitAndAssert(pk, expectedUdtInsert(pk));
  }

  @Test
  void testUdtInsertWithNull() {
    int pk = reservePk();
    udtInsertNull(pk);
    waitAndAssert(pk, expectedUdtInsertWithNull(pk));
  }

  @Test
  void testUdtInsertWithEmpty() {
    int pk = reservePk();
    udtInsertEmpty(pk);
    waitAndAssert(pk, expectedUdtInsertWithEmpty(pk));
  }

  @Test
  void testUdtDelete() {
    int pk = reservePk();
    udtInsertFull(pk);
    udtDeleteRow(pk);
    waitAndAssert(pk, expectedUdtDelete(pk));
  }

  @Test
  void testUdtUpdateFrozenUdtFromValueToValue() {
    int pk = reservePk();
    udtInsertForFrozenUpdate(pk);
    udtUpdateFrozenToValue(pk);
    waitAndAssert(pk, expectedUdtUpdateFrozenUdtFromValueToValue(pk));
  }

  @Test
  void testUdtUpdateFrozenUdtFromValueToNull() {
    int pk = reservePk();
    udtInsertForValueToNull(pk);
    udtUpdateAllToNull(pk);
    waitAndAssert(pk, expectedUdtUpdateFrozenUdtFromValueToNull(pk));
  }

  @Test
  void testUdtUpdateFrozenUdtFromValueToEmpty() {
    int pk = reservePk();
    udtInsertFull(pk);
    udtUpdateAllToEmpty(pk);
    waitAndAssert(pk, expectedUdtUpdateFrozenUdtFromValueToEmpty(pk));
  }

  @Test
  void testUdtUpdateFrozenUdtFromNullToValue() {
    int pk = reservePk();
    udtInsertNull(pk);
    udtUpdateFromNullToValue(pk);
    waitAndAssert(pk, expectedUdtUpdateFrozenUdtFromNullToValue(pk));
  }

  @Test
  void testUdtUpdateFrozenUdtFromEmptyToValue() {
    int pk = reservePk();
    udtInsertEmpty(pk);
    udtUpdateFromNullToValue(pk);
    waitAndAssert(pk, expectedUdtUpdateFrozenUdtFromEmptyToValue(pk));
  }

  @Test
  void testUdtUpdateNonFrozenUdtField() {
    int pk = reservePk();
    udtInsertForNonFrozenField(pk);
    udtUpdateNonFrozenField(pk);
    waitAndAssert(pk, expectedUdtUpdateNonFrozenUdtField(pk));
  }

  @Test
  void testUdtUpdateNonFrozenUdtFromValueToValue() {
    int pk = reservePk();
    udtInsertFull(pk);
    udtUpdateNonFrozenReplace(pk);
    waitAndAssert(pk, expectedUdtUpdateNonFrozenUdtFromValueToValue(pk));
  }

  @Test
  void testUdtUpdateNonFrozenUdtFromValueToNull() {
    int pk = reservePk();
    udtInsertFull(pk);
    udtUpdateNonFrozenToNull(pk);
    waitAndAssert(pk, expectedUdtUpdateNonFrozenUdtFromValueToNull(pk));
  }
}
