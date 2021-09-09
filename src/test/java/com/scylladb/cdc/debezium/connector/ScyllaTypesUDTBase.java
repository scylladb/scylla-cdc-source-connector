package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for UDT (User-Defined Types) replication. Tests insert, update, and delete
 * operations with both frozen and non-frozen UDTs.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesUDTBase<K, V> extends ScyllaTypesIT<K, V> {

  /** Returns expected records for inserting UDT columns. */
  abstract String[] expectedInsert(TestInfo testInfo);

  /** Returns expected records for deleting a row with UDT columns. */
  abstract String[] expectedDelete(TestInfo testInfo);

  // Expected insert values for update test setup
  /** Returns expected records after inserting a row with frozen UDTs. */
  abstract String[] expectedInsertWithFrozenUdt(TestInfo testInfo);

  /** Returns expected records after inserting a row with non-frozen UDTs. */
  abstract String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo);

  /** Returns expected records after inserting a row with nested non-frozen UDTs. */
  abstract String[] expectedInsertWithNonFrozenNestedUdt(TestInfo testInfo);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain maps. */
  abstract String[] expectedInsertWithNonFrozenUdtWithMap(TestInfo testInfo);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain lists. */
  abstract String[] expectedInsertWithNonFrozenUdtWithList(TestInfo testInfo);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain sets. */
  abstract String[] expectedInsertWithNonFrozenUdtWithSet(TestInfo testInfo);

  // Expected update values
  /** Returns expected records for updating frozen UDTs from value to value. */
  abstract String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo);

  /** Returns expected records for updating frozen UDTs from value to null. */
  abstract String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo);

  /** Returns expected records for updating a field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo);

  /** Returns expected records for updating a field in a nested non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenNestedUdtField(TestInfo testInfo);

  /** Returns expected records for updating a map field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithMapField(TestInfo testInfo);

  /** Returns expected records for updating a list field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithListField(TestInfo testInfo);

  /** Returns expected records for updating a set field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithSetField(TestInfo testInfo);

  /** Returns whether frozen UDT updates are expected for this test variant. */
  protected boolean expectFrozenUdtUpdates() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "frozen_udt frozen<simple_udt>,"
        + "nf_udt simple_udt,"
        + "frozen_nested_udt frozen<nested_udt>,"
        + "nf_nested_udt nested_udt,"
        + "frozen_udt_with_map frozen<udt_with_map>,"
        + "nf_udt_with_map udt_with_map,"
        + "frozen_udt_with_list frozen<udt_with_list>,"
        + "nf_udt_with_list udt_with_list,"
        + "frozen_udt_with_set frozen<udt_with_set>,"
        + "nf_udt_with_set udt_with_set"
        + ")";
  }

  /** Creates keyspace and UDT types required by the UDT tests. */
  @BeforeAll
  protected static void setup(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".simple_udt (a int, b text);");
    // Inner UDT for nested UDT test
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".inner_udt (x int, y text);");
    // Nested UDT containing another UDT
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    // UDT with map
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_map (m frozen<map<text, int>>);");
    // UDT with list
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_list (l frozen<list<int>>);");
    // UDT with set
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".udt_with_set (s frozen<set<text>>);");
  }

  /** Verifies an INSERT with UDT columns emits expected records. */
  @Test
  void testInsert(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + "1, "
            + "{a: 42, b: 'foo'}, "
            + "{a: 7, b: 'bar'}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    waitAndAssert(getConsumer(), expectedInsert(testInfo));
  }

  /** Verifies a DELETE emits expected records for UDT columns. */
  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + "1, "
            + "{a: 42, b: 'foo'}, "
            + "{a: 7, b: 'bar'}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  /** Verifies updates to frozen UDTs from value to value. */
  @Test
  void testUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + "1, "
            + "{a: 42, b: 'foo'}, "
            + "null, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    waitAndAssert(getConsumer(), expectedInsertWithFrozenUdt(testInfo));
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_udt = {a: 99, b: 'updated'}, "
            + "nf_udt.a = 77, "
            + "frozen_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
            + "nf_nested_udt.z = 21, "
            + "frozen_udt_with_map = {m: {'key1': 101, 'key3': 300}}, "
            + "nf_udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "frozen_udt_with_list = {l: [4, 5, 6]}, "
            + "nf_udt_with_list.l = [4, 5, 6], "
            + "frozen_udt_with_set = {s: {'d', 'e'}}, "
            + "nf_udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromValueToValue(testInfo);
      waitAndAssertFromCurrentPosition(getConsumer(), expected);
    }
  }

  /** Verifies updates to frozen UDTs from value to null. */
  @Test
  void testUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + "1, "
            + "{a: 42, b: 'foo'}, "
            + "null, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    waitAndAssert(getConsumer(), expectedInsertWithFrozenUdt(testInfo));
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_udt = null, "
            + "nf_udt = null, "
            + "frozen_nested_udt = null, "
            + "nf_nested_udt = null, "
            + "frozen_udt_with_map = null, "
            + "nf_udt_with_map = null, "
            + "frozen_udt_with_list = null, "
            + "nf_udt_with_list = null, "
            + "frozen_udt_with_set = null, "
            + "nf_udt_with_set = null "
            + "WHERE id = 1;");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromValueToNull(testInfo);
      waitAndAssertFromCurrentPosition(getConsumer(), expected);
    }
  }

  /** Verifies updates to a field within a non-frozen UDT. */
  @Test
  void testUpdateNonFrozenUdtField(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + "1, "
            + "null, "
            + "{a: 7, b: 'bar'}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{m: {'key1': 100, 'key2': 200}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    waitAndAssert(getConsumer(), expectedInsertWithNonFrozenUdt(testInfo));
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET nf_udt.a = 100, "
            + "frozen_udt = {a: 99, b: 'updated'}, "
            + "frozen_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
            + "nf_nested_udt.z = 21, "
            + "frozen_udt_with_map = {m: {'key1': 101, 'key3': 300}}, "
            + "nf_udt_with_map.m = {'key1': 101, 'key3': 300}, "
            + "frozen_udt_with_list = {l: [4, 5, 6]}, "
            + "nf_udt_with_list.l = [4, 5, 6], "
            + "frozen_udt_with_set = {s: {'d', 'e'}}, "
            + "nf_udt_with_set.s = {'d', 'e'} "
            + "WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenUdtField(testInfo);
    waitAndAssertFromCurrentPosition(getConsumer(), expected);
  }
}
