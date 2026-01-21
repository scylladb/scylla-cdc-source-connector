package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for non-frozen collections replication. Tests insert, update, and delete
 * operations with non-frozen collections (list, set, map).
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesNonFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

  abstract String[] expectedInsertWithValues(TestInfo testInfo);

  abstract String[] expectedInsertWithNull(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateListAddElement(TestInfo testInfo);

  abstract String[] expectedUpdateSetAddElement(TestInfo testInfo);

  abstract String[] expectedUpdateMapAddElement(TestInfo testInfo);

  abstract String[] expectedUpdateListRemoveElement(TestInfo testInfo);

  abstract String[] expectedUpdateSetRemoveElement(TestInfo testInfo);

  abstract String[] expectedUpdateMapRemoveElement(TestInfo testInfo);

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "list_col list<int>,"
        + "set_col set<text>,"
        + "map_col map<int, text>"
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
            + " (id, list_col, set_col, map_col) VALUES ("
            + "1, [10, 20, 30], {'x', 'y', 'z'}, {10: 'ten', 20: 'twenty'}"
            + ");");
    String[] expected = expectedInsertWithValues(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, list_col, set_col, map_col) VALUES (1, null, null, null);");
    String[] expected = expectedInsertWithNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, list_col, set_col, map_col) VALUES (1, [10], {'x'}, {10: 'ten'});");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateListAddElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO " + keyspaceTableName(testInfo) + " (id, list_col) VALUES (1, [10, 20]);");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET list_col = list_col + [30] WHERE id = 1;");
    String[] expected = expectedUpdateListAddElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateSetAddElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO " + keyspaceTableName(testInfo) + " (id, set_col) VALUES (1, {'x', 'y'});");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET set_col = set_col + {'z'} WHERE id = 1;");
    String[] expected = expectedUpdateSetAddElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateMapAddElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO " + keyspaceTableName(testInfo) + " (id, map_col) VALUES (1, {10: 'ten'});");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET map_col = map_col + {20: 'twenty'} WHERE id = 1;");
    String[] expected = expectedUpdateMapAddElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateListRemoveElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO " + keyspaceTableName(testInfo) + " (id, list_col) VALUES (1, [10, 20, 30]);");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET list_col = list_col - [20] WHERE id = 1;");
    String[] expected = expectedUpdateListRemoveElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateSetRemoveElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, set_col) VALUES (1, {'x', 'y', 'z'});");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET set_col = set_col - {'y'} WHERE id = 1;");
    String[] expected = expectedUpdateSetRemoveElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateMapRemoveElement(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, map_col) VALUES (1, {10: 'ten', 20: 'twenty'});");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET map_col = map_col - {10} WHERE id = 1;");
    String[] expected = expectedUpdateMapRemoveElement(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
