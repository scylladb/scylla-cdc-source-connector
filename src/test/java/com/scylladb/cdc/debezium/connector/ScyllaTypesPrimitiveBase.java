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

  @Test
  void testInsert() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'some text', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ")"
            + ";");
    String[] expected = expectedInsert(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'delete me', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk + ";");
    String[] expected = expectedDelete(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromValueToNil() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToNil(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromValueToEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToEmpty(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromValueToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value2', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', varint_col = 888888888 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromValueToValue(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromNilToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = 'ascii', bigint_col = 1234567890123, blob_col = 0xCAFEBABE, boolean_col = true, date_col = '2024-06-10', decimal_col = 12345.67, double_col = 3.14159, duration_col = 1d12h30m, float_col = 2.71828, inet_col = '127.0.0.1', int_col = 42, smallint_col = 7, text_col = 'value', time_col = '12:34:56.789', timestamp_col = '2024-06-10T12:34:56.789Z', timeuuid_col = 81d4a030-4632-11f0-9484-409dd8f36eba, tinyint_col = 5, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371c, varchar_col = 'varchar text', varint_col = 999999999 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromNilToValue(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromNilToEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromNilToEmpty(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromNilToNil() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromNilToNil(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromEmptyToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', varint_col = 888888888 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromEmptyToValue(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromEmptyToNil() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromEmptyToNil(pk);
    waitAndAssert(pk, expected);
  }

  @Test
  void testUpdateFromEmptyToEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + pk
            + ", '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateFromEmptyToEmpty(pk);
    waitAndAssert(pk, expected);
  }
}
