package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for primitive types replication. Tests simple insert operations with primitive
 * data types.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesPrimitiveBase<K, V> extends ScyllaTypesIT<K, V> {
  static final String UNTOUCHED_TEXT_VALUE = "untouched text";
  static final int UNTOUCHED_INT_VALUE = 101;
  static final boolean UNTOUCHED_BOOLEAN_VALUE = true;
  static final String UNTOUCHED_UUID_VALUE = "11111111-1111-1111-1111-111111111111";

  abstract String[] expectedInsert(int pk);

  abstract String[] expectedDelete(int pk);

  abstract String[] expectedUpdateFromValueToNil(int pk);

  abstract String[] expectedUpdateFromValueToEmpty(int pk);

  abstract String[] expectedUpdateFromValueToValue(int pk);

  abstract String[] expectedUpdateFromNilToValue(int pk);

  abstract String[] expectedUpdateFromNilToEmpty(int pk);

  abstract String[] expectedUpdateFromNilToNil(int pk);

  abstract String[] expectedUpdateFromEmptyToValue(int pk);

  abstract String[] expectedUpdateFromEmptyToNil(int pk);

  abstract String[] expectedUpdateFromEmptyToEmpty(int pk);

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
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
        + "untouched_uuid uuid"
        + ")";
  }

  // ===================== CQL Helper Methods =====================

  /** Column list for full row inserts. */
  private static final String ALL_COLUMNS =
      "id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, "
          + "duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, "
          + "timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, "
          + "untouched_text, untouched_int, untouched_boolean, untouched_uuid";

  /** Base values for full row (set 1 - original values). */
  private static final String BASE_VALUES_SET1 =
      "'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, "
          + "2.71828, '127.0.0.1', 42, 7, '%s', '12:34:56.789', '2024-06-10T12:34:56.789Z', "
          + "81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, "
          + "'varchar text', 999999999";

  /** Base values for empty string row (set 1 with empty strings). */
  private static final String EMPTY_VALUES_SET1 =
      "'', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, "
          + "2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', "
          + "81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, "
          + "'', 999999999";

  /** Base values for full row (set 2 - alternate values for updates). */
  private static final String BASE_VALUES_SET2 =
      "'ascii2', 1234567890124, 0xDEADBEEF, false, '2024-06-11', 98765.43, 2.71828, 2d1h, "
          + "1.41421, '127.0.0.2', 43, 8, '%s', '01:02:03.456', '2024-06-11T01:02:03.456Z', "
          + "81d4a031-4632-11f0-9484-409dd8f36eba, 6, 453662fa-db4b-4938-9033-d8523c0a371d, "
          + "'varchar text 2', 888888888";

  /** Empty values for full row (set 2 with empty strings). */
  private static final String EMPTY_VALUES_SET2 =
      "'', 1234567890124, 0xDEADBEEF, false, '2024-06-11', 98765.43, 2.71828, 2d1h, "
          + "1.41421, '127.0.0.2', 43, 8, '', '01:02:03.456', '2024-06-11T01:02:03.456Z', "
          + "81d4a031-4632-11f0-9484-409dd8f36eba, 6, 453662fa-db4b-4938-9033-d8523c0a371d, "
          + "'', 888888888";

  /** SET clause for updating all columns to null. */
  private static final String SET_ALL_NULL =
      "ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, "
          + "date_col = null, decimal_col = null, double_col = null, duration_col = null, "
          + "float_col = null, inet_col = null, int_col = null, smallint_col = null, "
          + "text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, "
          + "tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null";

  /** SET clause for updating to values set 1. */
  private static final String SET_VALUES_SET1 =
      "ascii_col = 'ascii', bigint_col = 1234567890123, blob_col = 0xCAFEBABE, "
          + "boolean_col = true, date_col = '2024-06-10', decimal_col = 12345.67, "
          + "double_col = 3.14159, duration_col = 1d12h30m, float_col = 2.71828, "
          + "inet_col = '127.0.0.1', int_col = 42, smallint_col = 7, text_col = 'value', "
          + "time_col = '12:34:56.789', timestamp_col = '2024-06-10T12:34:56.789Z', "
          + "timeuuid_col = 81d4a030-4632-11f0-9484-409dd8f36eba, tinyint_col = 5, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371c, varchar_col = 'varchar text', "
          + "varint_col = 999999999";

  /** SET clause for updating to values set 2 (with non-empty text). */
  private static final String SET_VALUES_SET2 =
      "ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, "
          + "boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, "
          + "double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, "
          + "inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value2', "
          + "time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', "
          + "timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', "
          + "varint_col = 888888888";

  /** SET clause for updating to values set 2 (with empty text). */
  private static final String SET_EMPTY_SET2 =
      "ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, "
          + "boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, "
          + "double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, "
          + "inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', "
          + "time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', "
          + "timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', "
          + "varint_col = 888888888";

  /** Inserts a full row with all columns using values set 1. */
  protected void insertFullRow(int pk, String textColValue) {
    String values = BASE_VALUES_SET1.formatted(textColValue);
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s, '%s', %d, %s, %s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                ALL_COLUMNS,
                pk,
                values,
                UNTOUCHED_TEXT_VALUE,
                UNTOUCHED_INT_VALUE,
                UNTOUCHED_BOOLEAN_VALUE,
                UNTOUCHED_UUID_VALUE));
  }

  /** Inserts a full row with empty strings for text columns. */
  protected void insertEmptyRow(int pk) {
    session.execute(
        "INSERT INTO %s (%s) VALUES (%d, %s, '%s', %d, %s, %s)"
            .formatted(
                getSuiteKeyspaceTableName(),
                ALL_COLUMNS,
                pk,
                EMPTY_VALUES_SET1,
                UNTOUCHED_TEXT_VALUE,
                UNTOUCHED_INT_VALUE,
                UNTOUCHED_BOOLEAN_VALUE,
                UNTOUCHED_UUID_VALUE));
  }

  /** Inserts a row with only the untouched columns (other columns will be null). */
  protected void insertOnlyUntouchedRow(int pk) {
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

  /** Updates all columns to null. */
  protected void updateToNull(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), SET_ALL_NULL, pk));
  }

  /** Updates to values set 1 (original values with text_col='value'). */
  protected void updateToValuesSet1(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), SET_VALUES_SET1, pk));
  }

  /** Updates to values set 2 (alternate values with text_col='value2'). */
  protected void updateToValuesSet2(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), SET_VALUES_SET2, pk));
  }

  /** Updates to empty values set 2 (alternate values with empty text columns). */
  protected void updateToEmptySet2(int pk) {
    session.execute(
        "UPDATE %s SET %s WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), SET_EMPTY_SET2, pk));
  }

  /** Deletes a row by primary key. */
  protected void deleteRow(int pk) {
    session.execute("DELETE FROM %s WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));
  }

  // ===================== Test Methods =====================

  @Test
  void testInsert() {
    int pk = reservePk();
    insertFullRow(pk, "some text");
    waitAndAssert(pk, expectedInsert(pk));
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    insertFullRow(pk, "delete me");
    deleteRow(pk);
    waitAndAssert(pk, expectedDelete(pk));
  }

  @Test
  void testUpdateFromValueToNil() {
    int pk = reservePk();
    insertFullRow(pk, "value");
    updateToNull(pk);
    waitAndAssert(pk, expectedUpdateFromValueToNil(pk));
  }

  @Test
  void testUpdateFromValueToEmpty() {
    int pk = reservePk();
    insertFullRow(pk, "value");
    updateToEmptySet2(pk);
    waitAndAssert(pk, expectedUpdateFromValueToEmpty(pk));
  }

  @Test
  void testUpdateFromValueToValue() {
    int pk = reservePk();
    insertFullRow(pk, "value");
    updateToValuesSet2(pk);
    waitAndAssert(pk, expectedUpdateFromValueToValue(pk));
  }

  @Test
  void testUpdateFromNilToValue() {
    int pk = reservePk();
    insertOnlyUntouchedRow(pk);
    updateToValuesSet1(pk);
    waitAndAssert(pk, expectedUpdateFromNilToValue(pk));
  }

  @Test
  void testUpdateFromNilToEmpty() {
    int pk = reservePk();
    insertOnlyUntouchedRow(pk);
    updateToEmptySet2(pk);
    waitAndAssert(pk, expectedUpdateFromNilToEmpty(pk));
  }

  @Test
  void testUpdateFromNilToNil() {
    int pk = reservePk();
    insertOnlyUntouchedRow(pk);
    updateToNull(pk);
    waitAndAssert(pk, expectedUpdateFromNilToNil(pk));
  }

  @Test
  void testUpdateFromEmptyToValue() {
    int pk = reservePk();
    insertEmptyRow(pk);
    updateToValuesSet2(pk);
    waitAndAssert(pk, expectedUpdateFromEmptyToValue(pk));
  }

  @Test
  void testUpdateFromEmptyToNil() {
    int pk = reservePk();
    insertEmptyRow(pk);
    updateToNull(pk);
    waitAndAssert(pk, expectedUpdateFromEmptyToNil(pk));
  }

  @Test
  void testUpdateFromEmptyToEmpty() {
    int pk = reservePk();
    insertEmptyRow(pk);
    updateToEmptySet2(pk);
    waitAndAssert(pk, expectedUpdateFromEmptyToEmpty(pk));
  }
}
