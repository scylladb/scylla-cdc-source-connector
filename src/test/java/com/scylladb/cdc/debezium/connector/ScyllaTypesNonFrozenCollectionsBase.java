package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for non-frozen collections replication. Tests insert, update, and delete
 * operations with non-frozen collections (list, set, map).
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesNonFrozenCollectionsBase<K, V> extends ScyllaTypesIT<K, V> {

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

  /** {@inheritDoc} */
  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "list_col list<int>,"
        + "set_col set<text>,"
        + "map_col map<int, text>"
        + ")";
  }

  /** Verifies an INSERT with collection values emits expected records. */
  @Test
  void testInsertWithValues() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, list_col, set_col, map_col) VALUES ("
            + pk
            + ", [10, 20, 30], {'x', 'y', 'z'}, {10: 'ten', 20: 'twenty'}"
            + ");");
    String[] expected = expectedInsertWithValues(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies an INSERT with null collections emits expected records. */
  @Test
  void testInsertWithNull() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, list_col, set_col, map_col) VALUES ("
            + pk
            + ", null, null, null);");
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
            + " (id, list_col, set_col, map_col) VALUES ("
            + pk
            + ", [10], {'x'}, {10: 'ten'});");
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk + ";");
    String[] expected = expectedDelete(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies list element additions emit expected delta records. */
  @Test
  void testUpdateListAddElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, list_col) VALUES ("
            + pk
            + ", [10, 20]);");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET list_col = list_col + [30] WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateListAddElement(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies set element additions emit expected delta records. */
  @Test
  void testUpdateSetAddElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, set_col) VALUES ("
            + pk
            + ", {'x', 'y'});");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET set_col = set_col + {'z'} WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateSetAddElement(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies map entry additions emit expected delta records. */
  @Test
  void testUpdateMapAddElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, map_col) VALUES ("
            + pk
            + ", {10: 'ten'});");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET map_col = map_col + {20: 'twenty'} WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateMapAddElement(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies list element removals emit expected delta records. */
  @Test
  void testUpdateListRemoveElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, list_col) VALUES ("
            + pk
            + ", [10, 20, 30]);");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET list_col = list_col - [20] WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateListRemoveElement(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies set element removals emit expected delta records. */
  @Test
  void testUpdateSetRemoveElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, set_col) VALUES ("
            + pk
            + ", {'x', 'y', 'z'});");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET set_col = set_col - {'y'} WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateSetRemoveElement(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies map entry removals emit expected delta records. */
  @Test
  void testUpdateMapRemoveElement() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, map_col) VALUES ("
            + pk
            + ", {10: 'ten', 20: 'twenty'});");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET map_col = map_col - {10} WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateMapRemoveElement(pk);
    waitAndAssert(pk, expected);
  }
}
