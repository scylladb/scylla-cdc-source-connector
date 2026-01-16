package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for frozen collections replication. Tests insert, update, and delete operations
 * with frozen collection types (list, set, map, tuple).
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String KEYSPACE = "frozen_collections_ks";

  abstract String[] expectedInsertWithValues();

  abstract String[] expectedInsertWithEmpty();

  abstract String[] expectedInsertWithNull();

  abstract String[] expectedDelete();

  abstract String[] expectedUpdateFromValueToValue();

  abstract String[] expectedUpdateFromValueToEmpty();

  abstract String[] expectedUpdateFromValueToNull();

  abstract String[] expectedUpdateFromEmptyToValue();

  abstract String[] expectedUpdateFromNullToValue();

  @Override
  protected String keyspace() {
    return KEYSPACE;
  }

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
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    String[] expected = expectedInsertWithValues();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithEmpty() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [], {}, {}, (null, null)"
            + ");");
    String[] expected = expectedInsertWithEmpty();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNull() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, null, null, null, (null, null)"
            + ");");
    String[] expected = expectedInsertWithNull();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute("DELETE FROM " + tableName() + " WHERE id = 1;");
    String[] expected = expectedDelete();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToValue() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_list_col = [4, 5, 6], frozen_set_col = {'x', 'y', 'z'}, "
            + "frozen_map_col = {3: 'three', 4: 'four'}, frozen_tuple_col = (99, 'bar') "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToValue();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToEmpty() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_list_col = [], frozen_set_col = {}, "
            + "frozen_map_col = {}, frozen_tuple_col = (null, null) "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToEmpty();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromValueToNull() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1, 2, 3], {'a', 'b', 'c'}, {1: 'one', 2: 'two'}, (42, 'foo')"
            + ");");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_list_col = null, frozen_set_col = null, "
            + "frozen_map_col = null, frozen_tuple_col = null "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromValueToNull();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromEmptyToValue() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [], {}, {}, (null, null)"
            + ");");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_list_col = [1, 2, 3], frozen_set_col = {'a', 'b', 'c'}, "
            + "frozen_map_col = {1: 'one', 2: 'two'}, frozen_tuple_col = (42, 'foo') "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromEmptyToValue();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFromNullToValue() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id) VALUES (1);");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_list_col = [1, 2, 3], frozen_set_col = {'a', 'b', 'c'}, "
            + "frozen_map_col = {1: 'one', 2: 'two'}, frozen_tuple_col = (42, 'foo') "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateFromNullToValue();
    waitAndAssert(getConsumer(), expected);
  }
}
