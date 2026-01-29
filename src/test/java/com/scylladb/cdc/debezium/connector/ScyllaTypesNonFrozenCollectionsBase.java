package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for non-frozen collections replication. Tests insert, update, and delete
 * operations with non-frozen collections (list, set, map).
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * <p>Non-frozen collections differ from frozen collections in that they support element-level
 * operations (add/remove individual elements) rather than only whole-collection replacement.
 * However, non-frozen maps cannot have UDTs or tuples as keys (only scalar types are allowed).
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesNonFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

  // Column names for inserts - includes all non-frozen collection columns with various key types
  private static final String INSERT_COLUMNS =
      "id, list_col, set_col, map_col, "
          + "map_ascii_col, map_bigint_col, map_boolean_col, "
          + "map_date_col, map_decimal_col, map_double_col, "
          + "map_float_col, map_int_col, map_inet_col, "
          + "map_smallint_col, map_text_col, map_time_col, "
          + "map_timestamp_col, map_timeuuid_col, map_tinyint_col, "
          + "map_uuid_col, map_varchar_col, map_varint_col, "
          + "set_timeuuid_col";

  private static String valuesWithValues(int pk) {
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

  private static String valuesWithNull(int pk) {
    return pk
        + ", null, null, null, "
        + "null, null, null, null, null, null, null, null, null, null, null, null, "
        + "null, null, null, null, null, null, null";
  }

  private static String valuesWithEmpty(int pk) {
    return pk
        + ", [], {}, {}, "
        + "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "
        + "{}, {}, {}, {}, {}, {}, {}";
  }

  private static final String UPDATE_SET_VALUES =
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

  private static final String UPDATE_SET_INITIAL_VALUES =
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

  private static final String UPDATE_SET_EMPTY =
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

  private static final String UPDATE_SET_NULL =
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

  /** Returns expected records for an INSERT with all collection values present. */
  abstract String[] expectedInsertWithValues(int pk);

  /** Returns expected records for an INSERT with all collection values set to null. */
  abstract String[] expectedInsertWithNull(int pk);

  /** Returns expected records for a DELETE of the row. */
  abstract String[] expectedDelete(int pk);

  /** Returns expected records for adding an element to a list column. */
  abstract String[] expectedUpdateListAddElement(int pk);

  /** Returns expected records for adding an element to a set column. */
  abstract String[] expectedUpdateSetAddElement(int pk);

  /** Returns expected records for adding an entry to a map column. */
  abstract String[] expectedUpdateMapAddElement(int pk);

  /** Returns expected records for removing an element from a list column. */
  abstract String[] expectedUpdateListRemoveElement(int pk);

  /** Returns expected records for removing an element from a set column. */
  abstract String[] expectedUpdateSetRemoveElement(int pk);

  /** Returns expected records for removing an entry from a map column. */
  abstract String[] expectedUpdateMapRemoveElement(int pk);

  /** Returns expected records for replacing entire collections with new values. */
  abstract String[] expectedUpdateFromValueToValue(int pk);

  /** Returns expected records for setting collections to null from populated values. */
  abstract String[] expectedUpdateFromValueToNull(int pk);

  /** Returns expected records for setting collections to empty from populated values. */
  abstract String[] expectedUpdateFromValueToEmpty(int pk);

  /** Returns expected records for updating collections from null to populated values. */
  abstract String[] expectedUpdateFromNullToValue(int pk);

  /** Returns expected records for updating collections from empty to populated values. */
  abstract String[] expectedUpdateFromEmptyToValue(int pk);

  @Override
  protected String createTableCql(String tableName) {
    // Non-frozen collections with various key types.
    // Note: Unlike frozen collections, non-frozen maps cannot have UDTs or tuples as keys.
    return "("
        + "id int PRIMARY KEY,"
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
        + "set_timeuuid_col set<timeuuid>"
        + ")";
  }

  // ===================== CQL Helper Methods =====================

  /** Inserts a row with all collection values. */
  protected void insertWithValues(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(getSuiteKeyspaceTableName(), INSERT_COLUMNS, valuesWithValues(pk)));
  }

  /** Inserts a row with null collection values. */
  protected void insertWithNull(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(getSuiteKeyspaceTableName(), INSERT_COLUMNS, valuesWithNull(pk)));
  }

  /** Inserts a row with empty collection values. */
  protected void insertWithEmpty(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%s)"
            .formatted(getSuiteKeyspaceTableName(), INSERT_COLUMNS, valuesWithEmpty(pk)));
  }

  /** Inserts a row with minimal collection values (for delete/update tests). */
  protected void insertMinimal(int pk) {
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, [10], {'x'}, {10: 'ten'})"
            .formatted(getSuiteKeyspaceTableName(), pk));
  }

  /** Inserts a row with only list column. */
  protected void insertListOnly(int pk, String listValue) {
    session.execute(
        "INSERT INTO %s (id, list_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, listValue));
  }

  /** Inserts a row with only set column. */
  protected void insertSetOnly(int pk, String setValue) {
    session.execute(
        "INSERT INTO %s (id, set_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, setValue));
  }

  /** Inserts a row with only map column. */
  protected void insertMapOnly(int pk, String mapValue) {
    session.execute(
        "INSERT INTO %s (id, map_col) VALUES (%d, %s)"
            .formatted(getSuiteKeyspaceTableName(), pk, mapValue));
  }

  /** Deletes a row by primary key. */
  protected void deleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  /** Appends elements to list column. */
  protected void updateListAppend(int pk, String elements) {
    session.execute(
        "UPDATE %s SET list_col = list_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  /** Removes elements from list column. */
  protected void updateListRemove(int pk, String elements) {
    session.execute(
        "UPDATE %s SET list_col = list_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  /** Adds elements to set column. */
  protected void updateSetAdd(int pk, String elements) {
    session.execute(
        "UPDATE %s SET set_col = set_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  /** Removes elements from set column. */
  protected void updateSetRemove(int pk, String elements) {
    session.execute(
        "UPDATE %s SET set_col = set_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), elements, pk));
  }

  /** Adds entries to map column. */
  protected void updateMapAdd(int pk, String entries) {
    session.execute(
        "UPDATE %s SET map_col = map_col + %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), entries, pk));
  }

  /** Removes keys from map column. */
  protected void updateMapRemove(int pk, String keys) {
    session.execute(
        "UPDATE %s SET map_col = map_col - %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), keys, pk));
  }

  /** Updates all collection columns to new values (whole-collection replacement). */
  protected void updateToValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), UPDATE_SET_VALUES, pk));
  }

  /** Updates all collection columns to initial values (for testing empty/null to value). */
  protected void updateToInitialValues(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), UPDATE_SET_INITIAL_VALUES, pk));
  }

  /** Updates all collection columns to null. */
  protected void updateToNull(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), UPDATE_SET_NULL, pk));
  }

  /** Updates all collection columns to empty collections. */
  protected void updateToEmpty(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), UPDATE_SET_EMPTY, pk));
  }

  // ===================== Test Methods =====================

  @Test
  void testInsertWithValues() {
    int pk = reservePk();
    insertWithValues(pk);
    waitAndAssert(pk, expectedInsertWithValues(pk));
  }

  @Test
  void testInsertWithNull() {
    int pk = reservePk();
    insertWithNull(pk);
    waitAndAssert(pk, expectedInsertWithNull(pk));
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    insertMinimal(pk);
    deleteRow(pk);
    waitAndAssert(pk, expectedDelete(pk));
  }

  @Test
  void testUpdateListAddElement() {
    int pk = reservePk();
    insertListOnly(pk, "[10, 20]");
    updateListAppend(pk, "[30]");
    waitAndAssert(pk, expectedUpdateListAddElement(pk));
  }

  @Test
  void testUpdateSetAddElement() {
    int pk = reservePk();
    insertSetOnly(pk, "{'x', 'y'}");
    updateSetAdd(pk, "{'z'}");
    waitAndAssert(pk, expectedUpdateSetAddElement(pk));
  }

  @Test
  void testUpdateMapAddElement() {
    int pk = reservePk();
    insertMapOnly(pk, "{10: 'ten'}");
    updateMapAdd(pk, "{20: 'twenty'}");
    waitAndAssert(pk, expectedUpdateMapAddElement(pk));
  }

  @Test
  void testUpdateListRemoveElement() {
    int pk = reservePk();
    insertListOnly(pk, "[10, 20, 30]");
    updateListRemove(pk, "[20]");
    waitAndAssert(pk, expectedUpdateListRemoveElement(pk));
  }

  @Test
  void testUpdateSetRemoveElement() {
    int pk = reservePk();
    insertSetOnly(pk, "{'x', 'y', 'z'}");
    updateSetRemove(pk, "{'y'}");
    waitAndAssert(pk, expectedUpdateSetRemoveElement(pk));
  }

  @Test
  void testUpdateMapRemoveElement() {
    int pk = reservePk();
    insertMapOnly(pk, "{10: 'ten', 20: 'twenty'}");
    updateMapRemove(pk, "{10}");
    waitAndAssert(pk, expectedUpdateMapRemoveElement(pk));
  }

  @Test
  void testUpdateFromValueToValue() {
    int pk = reservePk();
    insertWithValues(pk);
    updateToValues(pk);
    waitAndAssert(pk, expectedUpdateFromValueToValue(pk));
  }

  @Test
  void testUpdateFromValueToNull() {
    int pk = reservePk();
    insertWithValues(pk);
    updateToNull(pk);
    waitAndAssert(pk, expectedUpdateFromValueToNull(pk));
  }

  @Test
  void testUpdateFromValueToEmpty() {
    int pk = reservePk();
    insertWithValues(pk);
    updateToEmpty(pk);
    waitAndAssert(pk, expectedUpdateFromValueToEmpty(pk));
  }

  @Test
  void testUpdateFromNullToValue() {
    int pk = reservePk();
    insertWithNull(pk);
    updateToInitialValues(pk);
    waitAndAssert(pk, expectedUpdateFromNullToValue(pk));
  }

  @Test
  void testUpdateFromEmptyToValue() {
    int pk = reservePk();
    insertWithEmpty(pk);
    updateToInitialValues(pk);
    waitAndAssert(pk, expectedUpdateFromEmptyToValue(pk));
  }
}
