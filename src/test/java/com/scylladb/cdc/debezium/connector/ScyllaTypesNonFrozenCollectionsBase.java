package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for non-frozen collections replication. Tests insert, update, and delete
 * operations with non-frozen collections (list, set, map).
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesNonFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String KEYSPACE = "nonfrozen_collections_ks";

  abstract String[] expectedInsertWithValues();

  abstract String[] expectedInsertWithNull();

  abstract String[] expectedDelete();

  abstract String[] expectedUpdateListAddElement();

  abstract String[] expectedUpdateSetAddElement();

  abstract String[] expectedUpdateMapAddElement();

  abstract String[] expectedUpdateListRemoveElement();

  abstract String[] expectedUpdateSetRemoveElement();

  abstract String[] expectedUpdateMapRemoveElement();

  @Override
  protected String keyspace() {
    return KEYSPACE;
  }

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
  protected static void setup() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + KEYSPACE
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
  }

  @Test
  void testInsertWithValues() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, list_col, set_col, map_col) VALUES ("
            + "1, [10, 20, 30], {'x', 'y', 'z'}, {10: 'ten', 20: 'twenty'}"
            + ");");
    String[] expected = expectedInsertWithValues();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNull() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, list_col, set_col, map_col) VALUES (1, null, null, null);");
    String[] expected = expectedInsertWithNull();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, list_col, set_col, map_col) VALUES (1, [10], {'x'}, {10: 'ten'});");
    session.execute("DELETE FROM " + tableName() + " WHERE id = 1;");
    String[] expected = expectedDelete();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateListAddElement() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id, list_col) VALUES (1, [10, 20]);");
    session.execute("UPDATE " + tableName() + " SET list_col = list_col + [30] WHERE id = 1;");
    String[] expected = expectedUpdateListAddElement();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateSetAddElement() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id, set_col) VALUES (1, {'x', 'y'});");
    session.execute("UPDATE " + tableName() + " SET set_col = set_col + {'z'} WHERE id = 1;");
    String[] expected = expectedUpdateSetAddElement();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateMapAddElement() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id, map_col) VALUES (1, {10: 'ten'});");
    session.execute(
        "UPDATE " + tableName() + " SET map_col = map_col + {20: 'twenty'} WHERE id = 1;");
    String[] expected = expectedUpdateMapAddElement();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateListRemoveElement() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id, list_col) VALUES (1, [10, 20, 30]);");
    session.execute("UPDATE " + tableName() + " SET list_col = list_col - [20] WHERE id = 1;");
    String[] expected = expectedUpdateListRemoveElement();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateSetRemoveElement() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id, set_col) VALUES (1, {'x', 'y', 'z'});");
    session.execute("UPDATE " + tableName() + " SET set_col = set_col - {'y'} WHERE id = 1;");
    String[] expected = expectedUpdateSetRemoveElement();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateMapRemoveElement() {
    truncateTables();
    session.execute(
        "INSERT INTO " + tableName() + " (id, map_col) VALUES (1, {10: 'ten', 20: 'twenty'});");
    session.execute("UPDATE " + tableName() + " SET map_col = map_col - {10} WHERE id = 1;");
    String[] expected = expectedUpdateMapRemoveElement();
    waitAndAssert(getConsumer(), expected);
  }
}
