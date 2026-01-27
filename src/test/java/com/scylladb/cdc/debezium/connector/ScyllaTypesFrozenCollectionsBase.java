package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for frozen collections replication. Tests insert, update, and delete operations
 * with frozen collection types (list, set, map, tuple).
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

  private static final String INSERT_COLUMNS =
      "id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col, "
          + "frozen_map_ascii_col, frozen_map_bigint_col, frozen_map_boolean_col, "
          + "frozen_map_date_col, frozen_map_decimal_col, frozen_map_double_col, "
          + "frozen_map_float_col, frozen_map_int_col, frozen_map_inet_col, "
          + "frozen_map_smallint_col, frozen_map_text_col, frozen_map_time_col, "
          + "frozen_map_timestamp_col, frozen_map_timeuuid_col, frozen_map_tinyint_col, "
          + "frozen_map_uuid_col, frozen_map_varchar_col, frozen_map_varint_col, "
          + "frozen_map_tuple_key_col, frozen_map_udt_key_col";

  private static String valuesWithValues(int pk) {
    return pk + ", [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo'), "
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
        + "{(1, 'tuple_key'): 'tuple_value'}, {{a: 1, b: 'udt_key'}: 'udt_value'}";
  }

  private static String valuesWithEmpty(int pk) {
    return pk + ", [], {}, {}, (null, null), "
        + "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}";
  }

  private static String valuesWithNull(int pk) {
    return pk + ", null, null, null, (null, null), "
        + "null, null, null, null, null, null, null, null, null, null, null, null, "
        + "null, null, null, null, null, null, null, null";
  }

  private static final String UPDATE_SET_VALUES =
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
          + "frozen_map_udt_key_col = {{a: 2, b: 'udt_key_2'}: 'udt_value_2'}";

  private static final String UPDATE_SET_INITIAL_VALUES =
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
          + "frozen_map_udt_key_col = {{a: 1, b: 'udt_key'}: 'udt_value'}";

  private static final String UPDATE_SET_EMPTY =
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
          + "frozen_map_tuple_key_col = {}, frozen_map_udt_key_col = {}";

  private static final String UPDATE_SET_NULL =
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
          + "frozen_map_tuple_key_col = null, frozen_map_udt_key_col = null";

  /** Returns expected records for an INSERT with all frozen collection values present. */
  abstract String[] expectedInsertWithValues(int pk);

  /** Returns expected records for an INSERT with empty frozen collections. */
  abstract String[] expectedInsertWithEmpty(int pk);

  /** Returns expected records for an INSERT with null frozen collections. */
  abstract String[] expectedInsertWithNull(int pk);

  /** Returns expected records for a DELETE of the row. */
  abstract String[] expectedDelete(int pk);

  /** Returns expected records for updating from values to values. */
  abstract String[] expectedUpdateFromValueToValue(int pk);

  /** Returns expected records for updating from values to empty collections. */
  abstract String[] expectedUpdateFromValueToEmpty(int pk);

  /** Returns expected records for updating from values to null collections. */
  abstract String[] expectedUpdateFromValueToNull(int pk);

  /** Returns expected records for updating from empty collections to values. */
  abstract String[] expectedUpdateFromEmptyToValue(int pk);

  /** Returns expected records for updating from null collections to values. */
  abstract String[] expectedUpdateFromNullToValue(int pk);

  /** {@inheritDoc} */
  @Override
  protected void createTypesBeforeTable(String keyspaceName) {
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".map_key_udt (a int, b text);");
  }

  /** {@inheritDoc} */
  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
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
        + "frozen_map_udt_key_col frozen<map<frozen<map_key_udt>, text>>"
        + ")";
  }

  /** Verifies an INSERT with frozen collection values emits expected records. */
  @Test
  void testInsertWithValues() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithValues(pk)
            + ");");
    String[] expected = expectedInsertWithValues(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies an INSERT with empty frozen collections emits expected records. */
  @Test
  void testInsertWithEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithEmpty(pk)
            + ");");
    String[] expected = expectedInsertWithEmpty(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies an INSERT with null frozen collections emits expected records. */
  @Test
  void testInsertWithNull() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithNull(pk)
            + ");");
    String[] expected = expectedInsertWithNull(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies a DELETE emits expected records. */
  @Test
  void testDelete() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithValues(pk)
            + ");");
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk + ";");
    String[] expected = expectedDelete(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates from populated values to populated values emit expected records. */
  @Test
  void testUpdateFromValueToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithValues(pk)
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + UPDATE_SET_VALUES
            + " WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToValue(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates from populated values to empty collections emit expected records. */
  @Test
  void testUpdateFromValueToEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithValues(pk)
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + UPDATE_SET_EMPTY
            + " WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToEmpty(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates from populated values to null collections emit expected records. */
  @Test
  void testUpdateFromValueToNull() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithValues(pk)
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + UPDATE_SET_NULL
            + " WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToNull(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates from empty collections to populated values emit expected records. */
  @Test
  void testUpdateFromEmptyToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + valuesWithEmpty(pk)
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + UPDATE_SET_INITIAL_VALUES
            + " WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromEmptyToValue(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates from null collections to populated values emit expected records. */
  @Test
  void testUpdateFromNullToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO " + getSuiteKeyspaceTableName() + " (id) VALUES (" + pk + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + UPDATE_SET_INITIAL_VALUES
            + " WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromNullToValue(pk);
    waitAndAssert(pk, expected);
  }
}
