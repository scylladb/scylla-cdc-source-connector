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
public abstract class ScyllaTypesFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

  abstract String[] expectedInsertWithValues(TestInfo testInfo);

  abstract String[] expectedInsertWithEmpty(TestInfo testInfo);

  abstract String[] expectedInsertWithNull(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToEmpty(TestInfo testInfo);

  abstract String[] expectedUpdateFromValueToNull(TestInfo testInfo);

  abstract String[] expectedUpdateFromEmptyToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFromNullToValue(TestInfo testInfo);

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "frozen_list_col frozen<list<int>>,"
        + "frozen_set_col frozen<set<text>>,"
        + "frozen_map_col frozen<map<int, text>>,"
        + "frozen_tuple_col frozen<tuple<int, text>>"
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
  void testInsertWithValues(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
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
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [], {}, {}, (null, null)"
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
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, null, null, null, (null, null)"
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
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
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
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_list_col = [4, 5, 6], frozen_set_col = {'x', 'y', 'z'}, "
            + "frozen_map_col = {3: 'three', 4: 'four'}, frozen_tuple_col = (99, 'bar') "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToEmpty(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_list_col = [], frozen_set_col = {}, "
            + "frozen_map_col = {}, frozen_tuple_col = (null, null) "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToEmpty(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_list_col = null, frozen_set_col = null, "
            + "frozen_map_col = null, frozen_tuple_col = null "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [], {}, {}, (null, null)"
            + ");");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_list_col = [1, 2, 3], frozen_set_col = {'a', 'b', 'c'}, "
            + "frozen_map_col = {1: 'one', 2: 'two'}, frozen_tuple_col = (42, 'foo') "
            + "WHERE id = 1;");
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
            + " SET frozen_list_col = [1, 2, 3], frozen_set_col = {'a', 'b', 'c'}, "
            + "frozen_map_col = {1: 'one', 2: 'two'}, frozen_tuple_col = (42, 'foo') "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromNullToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
