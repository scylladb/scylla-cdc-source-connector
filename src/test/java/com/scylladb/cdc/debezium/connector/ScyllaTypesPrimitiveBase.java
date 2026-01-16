package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for primitive types replication. Tests simple insert operations with primitive
 * data types.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesPrimitiveBase<K, V> extends ScyllaTypesIT<K, V> {
  static final String UNTOUCHED_TEXT_VALUE = "untouched text";
  static final int UNTOUCHED_INT_VALUE = 101;
  static final boolean UNTOUCHED_BOOLEAN_VALUE = true;
  static final String UNTOUCHED_UUID_VALUE = "11111111-1111-1111-1111-111111111111";

  abstract String[] expectedInsert(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToNil(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToEmpty(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromNilToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromNilToEmpty(TestInfo testInfo);

  abstract String[] expectedUpdateFromNilToNil(TestInfo testInfo);

  abstract String[] expectedUpdateFromEmptyToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromEmptyToNil(TestInfo testInfo);

  abstract String[] expectedUpdateFromEmptyToEmpty(TestInfo testInfo);

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

  @BeforeAll
  protected static void setup(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
  }

  @Test
  void testInsert(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'some text', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ")"
            + ";");
    String[] expected = expectedInsert(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'delete me', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
            + UNTOUCHED_TEXT_VALUE
            + "', "
            + UNTOUCHED_INT_VALUE
            + ", "
            + UNTOUCHED_BOOLEAN_VALUE
            + ", "
            + UNTOUCHED_UUID_VALUE
            + ");");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToNil(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToNil(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'value', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value2', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', varint_col = 888888888 WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromNilToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES (1, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = 'ascii', bigint_col = 1234567890123, blob_col = 0xCAFEBABE, boolean_col = true, date_col = '2024-06-10', decimal_col = 12345.67, double_col = 3.14159, duration_col = 1d12h30m, float_col = 2.71828, inet_col = '127.0.0.1', int_col = 42, smallint_col = 7, text_col = 'value', time_col = '12:34:56.789', timestamp_col = '2024-06-10T12:34:56.789Z', timeuuid_col = 81d4a030-4632-11f0-9484-409dd8f36eba, tinyint_col = 5, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371c, varchar_col = 'varchar text', varint_col = 999999999 WHERE id = 1;");
    String[] expected = expectedUpdateFromNilToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromNilToEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES (1, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = 1;");
    String[] expected = expectedUpdateFromNilToEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromNilToNil(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES (1, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = 1;");
    String[] expected = expectedUpdateFromNilToNil(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = 'ascii2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'value', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar text 2', varint_col = 888888888 WHERE id = 1;");
    String[] expected = expectedUpdateFromEmptyToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToNil(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = null, bigint_col = null, blob_col = null, boolean_col = null, date_col = null, decimal_col = null, double_col = null, duration_col = null, float_col = null, inet_col = null, int_col = null, smallint_col = null, text_col = null, time_col = null, timestamp_col = null, timeuuid_col = null, tinyint_col = null, uuid_col = null, varchar_col = null, varint_col = null WHERE id = 1;");
    String[] expected = expectedUpdateFromEmptyToNil(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, untouched_text, untouched_int, untouched_boolean, untouched_uuid) VALUES ("
            + "1, '', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, '', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, '', 999999999, '"
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
            + keyspaceTableName(testInfo)
            + " SET ascii_col = '', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = '', time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = '', varint_col = 888888888 WHERE id = 1;");
    String[] expected = expectedUpdateFromEmptyToEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
