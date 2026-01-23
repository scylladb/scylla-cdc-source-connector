package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for frozen collections replication. Tests insert, update, and delete operations
 * with frozen collection types (list, set, map, tuple).
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

  private static final String INSERT_COLUMNS =
      "id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col, "
          + "frozen_map_ascii_col, frozen_map_bigint_col, frozen_map_boolean_col, "
          + "frozen_map_date_col, frozen_map_decimal_col, frozen_map_double_col, "
          + "frozen_map_float_col, frozen_map_int_col, frozen_map_inet_col, "
          + "frozen_map_smallint_col, frozen_map_text_col, frozen_map_time_col, "
          + "frozen_map_timestamp_col, frozen_map_timeuuid_col, frozen_map_tinyint_col, "
          + "frozen_map_uuid_col, frozen_map_varchar_col, frozen_map_varint_col, "
          + "frozen_map_tuple_key_col, frozen_map_udt_key_col, frozen_udt, "
          + "frozen_nested_udt, frozen_udt_with_map, frozen_udt_with_list, frozen_udt_with_set, "
          + "list_col, set_col, map_col, udt, nested_udt, udt_with_map, "
          + "udt_with_list, udt_with_set";

  private static final String VALUES_WITH_VALUES =
      "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo'), "
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
          + "{a: 42, b: 'foo'}, {inner: {x: 10, y: 'hello'}, z: 20}, "
          + "{m: {'key1': 100, 'key2': 200}}, {l: [1, 2, 3]}, {s: {'a', 'b', 'c'}}, "
          + "[10, 20, 30], {'x', 'y', 'z'}, {10: 'ten', 20: 'twenty'}, "
          + "{a: 7, b: 'bar'}, {inner: {x: 10, y: 'hello'}, z: 20}, "
          + "{m: {'key1': 100, 'key2': 200}}, {l: [1, 2, 3]}, {s: {'a', 'b', 'c'}}";

  private static final String VALUES_WITH_EMPTY =
      "1, [], {}, {}, (null, null), "
          + "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}"
          + ", {a: null, b: null}, {inner: null, z: null}, {m: {}}, {l: []}, {s: {}}, "
          + "[], {}, {}, {a: null, b: null}, {inner: null, z: null}, {m: {}}, {l: []}, {s: {}}";

  private static final String VALUES_WITH_NULL =
      "1, null, null, null, (null, null), "
          + "null, null, null, null, null, null, null, null, null, null, null, null, "
          + "null, null, null, null, null, null, null, null, null, null, null, null, null, "
          + "null, null, null, null, null, null, null, null";

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
          + "frozen_map_udt_key_col = {{a: 2, b: 'udt_key_2'}: 'udt_value_2'}, "
          + "frozen_udt = {a: 99, b: 'updated'}, "
          + "frozen_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
          + "frozen_udt_with_map = {m: {'key1': 101, 'key3': 300}}, "
          + "frozen_udt_with_list = {l: [4, 5, 6]}, "
          + "frozen_udt_with_set = {s: {'d', 'e'}}, "
          + "list_col = [40, 50, 60], set_col = {'p', 'q', 'r'}, "
          + "map_col = {30: 'thirty', 40: 'forty'}, udt = {a: 100, b: 'updated'}, "
          + "nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
          + "udt_with_map = {m: {'key1': 101, 'key3': 300}}, "
          + "udt_with_list = {l: [4, 5, 6]}, udt_with_set = {s: {'d', 'e'}}";

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
          + "frozen_map_udt_key_col = {{a: 1, b: 'udt_key'}: 'udt_value'}, "
          + "frozen_udt = {a: 42, b: 'foo'}, "
          + "frozen_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
          + "frozen_udt_with_map = {m: {'key1': 100, 'key2': 200}}, "
          + "frozen_udt_with_list = {l: [1, 2, 3]}, "
          + "frozen_udt_with_set = {s: {'a', 'b', 'c'}}, "
          + "list_col = [10, 20, 30], set_col = {'x', 'y', 'z'}, "
          + "map_col = {10: 'ten', 20: 'twenty'}, udt = {a: 7, b: 'bar'}, "
          + "nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
          + "udt_with_map = {m: {'key1': 100, 'key2': 200}}, "
          + "udt_with_list = {l: [1, 2, 3]}, udt_with_set = {s: {'a', 'b', 'c'}}";

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
          + "frozen_map_tuple_key_col = {}, frozen_map_udt_key_col = {}, "
          + "frozen_udt = {a: null, b: null}, "
          + "frozen_nested_udt = {inner: null, z: null}, "
          + "frozen_udt_with_map = {m: {}}, "
          + "frozen_udt_with_list = {l: []}, "
          + "frozen_udt_with_set = {s: {}}, "
          + "list_col = [], set_col = {}, map_col = {}, udt = {a: null, b: null}, "
          + "nested_udt = {inner: null, z: null}, udt_with_map = {m: {}}, "
          + "udt_with_list = {l: []}, udt_with_set = {s: {}}";

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
          + "frozen_map_tuple_key_col = null, frozen_map_udt_key_col = null, "
          + "frozen_udt = null, frozen_nested_udt = null, frozen_udt_with_map = null, "
          + "frozen_udt_with_list = null, frozen_udt_with_set = null, "
          + "list_col = null, set_col = null, map_col = null, udt = null, "
          + "nested_udt = null, udt_with_map = null, udt_with_list = null, "
          + "udt_with_set = null";

  abstract String[] expectedInsertWithValues(TestInfo testInfo);

  abstract String[] expectedInsertWithEmpty(TestInfo testInfo);

  abstract String[] expectedInsertWithNull(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToEmpty(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToNull(TestInfo testInfo);

  abstract String[] expectedUpdateFromEmptyToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromNullToValue(TestInfo testInfo);

  abstract String[] expectedNonFrozenAddElement(TestInfo testInfo);

  abstract String[] expectedNonFrozenAddElementFromNull(TestInfo testInfo);

  abstract String[] expectedNonFrozenAddElementFromEmpty(TestInfo testInfo);

  abstract String[] expectedNonFrozenRemoveElement(TestInfo testInfo);

  abstract String[] expectedNonFrozenRemoveAllElements(TestInfo testInfo);

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
        + "frozen_map_udt_key_col frozen<map<frozen<map_key_udt>, text>>,"
        + "frozen_udt frozen<simple_udt>,"
        + "frozen_nested_udt frozen<nested_udt>,"
        + "frozen_udt_with_map frozen<udt_with_map>,"
        + "frozen_udt_with_list frozen<udt_with_list>,"
        + "frozen_udt_with_set frozen<udt_with_set>,"
        + "list_col list<int>,"
        + "set_col set<text>,"
        + "map_col map<int, text>,"
        + "udt simple_udt,"
        + "nested_udt nested_udt,"
        + "udt_with_map udt_with_map,"
        + "udt_with_list udt_with_list,"
        + "udt_with_set udt_with_set"
        + ")";
  }

  @BeforeAll
  protected static void setup(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".map_key_udt (a int, b text);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".simple_udt (a int, b text);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".inner_udt (x int, y text);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_map (m frozen<map<text, int>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_list (l frozen<list<int>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_set (s frozen<set<text>>);");
  }

  @Test
  void testInsertWithValues(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    String[] expected = expectedInsertWithValues(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_EMPTY
            + ");");
    String[] expected = expectedInsertWithEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_NULL
            + ");");
    String[] expected = expectedInsertWithNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET " + UPDATE_SET_VALUES + " WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET " + UPDATE_SET_EMPTY + " WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET " + UPDATE_SET_NULL + " WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_EMPTY
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET "
            + UPDATE_SET_INITIAL_VALUES
            + " WHERE id = 1;");
    String[] expected = expectedUpdateFromEmptyToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromNullToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute("INSERT INTO " + keyspaceTableName(testInfo) + " (id) VALUES (1);");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET "
            + UPDATE_SET_INITIAL_VALUES
            + " WHERE id = 1;");
    String[] expected = expectedUpdateFromNullToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testNonFrozenAddElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET list_col = list_col + [40], "
            + "set_col = set_col + {'w'}, "
            + "map_col = map_col + {30: 'thirty'}, "
            + "udt.a = 100, "
            + "nested_udt.z = 21, "
            + "udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "udt_with_list.l = [4, 5, 6], "
            + "udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    String[] expected = expectedNonFrozenAddElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testNonFrozenAddElementFromNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_NULL
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET list_col = list_col + [40], "
            + "set_col = set_col + {'w'}, "
            + "map_col = map_col + {30: 'thirty'}, "
            + "udt.a = 100, "
            + "nested_udt.z = 21, "
            + "udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "udt_with_list.l = [4, 5, 6], "
            + "udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    String[] expected = expectedNonFrozenAddElementFromNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testNonFrozenAddElementFromEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_EMPTY
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET list_col = list_col + [40], "
            + "set_col = set_col + {'w'}, "
            + "map_col = map_col + {30: 'thirty'}, "
            + "udt.a = 100, "
            + "nested_udt.z = 21, "
            + "udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "udt_with_list.l = [4, 5, 6], "
            + "udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    String[] expected = expectedNonFrozenAddElementFromEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testNonFrozenRemoveElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET list_col = list_col - [20], "
            + "set_col = set_col - {'y'}, "
            + "map_col = map_col - {10}, "
            + "udt.a = 100, "
            + "nested_udt.z = 21, "
            + "udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "udt_with_list.l = [4, 5, 6], "
            + "udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    String[] expected = expectedNonFrozenRemoveElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testNonFrozenRemoveAllElements(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " ("
            + INSERT_COLUMNS
            + ") VALUES ("
            + VALUES_WITH_VALUES
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET list_col = list_col - [10, 20, 30], "
            + "set_col = set_col - {'x', 'y', 'z'}, "
            + "map_col = map_col - {10, 20}, "
            + "udt.a = null, "
            + "udt.b = null, "
            + "nested_udt.inner = null, "
            + "nested_udt.z = null, "
            + "udt_with_map.m = {}, "
            + "udt_with_list.l = [], "
            + "udt_with_set.s = {} "
            + "WHERE id = 1;");
    String[] expected = expectedNonFrozenRemoveAllElements(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
