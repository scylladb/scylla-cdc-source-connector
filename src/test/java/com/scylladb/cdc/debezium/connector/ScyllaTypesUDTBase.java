package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for UDT (User-Defined Types) replication. Tests insert, update, and delete
 * operations with both frozen and non-frozen UDTs.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesUDTBase<K, V> extends ScyllaTypesIT<K, V> {

  /** Returns expected records for inserting UDT columns. */
  abstract String[] expectedInsert(int pk);

  /** Returns expected records for deleting a row with UDT columns. */
  abstract String[] expectedDelete(int pk);

  // Expected insert values for update test setup
  /** Returns expected records after inserting a row with frozen UDTs. */
  abstract String[] expectedInsertWithFrozenUdt(int pk);

  /** Returns expected records after inserting a row with non-frozen UDTs. */
  abstract String[] expectedInsertWithNonFrozenUdt(int pk);

  /** Returns expected records after inserting a row with nested non-frozen UDTs. */
  abstract String[] expectedInsertWithNonFrozenNestedUdt(int pk);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain maps. */
  abstract String[] expectedInsertWithNonFrozenUdtWithMap(int pk);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain lists. */
  abstract String[] expectedInsertWithNonFrozenUdtWithList(int pk);

  /** Returns expected records after inserting a row with non-frozen UDTs that contain sets. */
  abstract String[] expectedInsertWithNonFrozenUdtWithSet(int pk);

  // Expected update values
  /** Returns expected records for updating frozen UDTs from value to value. */
  abstract String[] expectedUpdateFrozenUdtFromValueToValue(int pk);

  /** Returns expected records for updating frozen UDTs from value to null. */
  abstract String[] expectedUpdateFrozenUdtFromValueToNull(int pk);

  /** Returns expected records for updating a field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtField(int pk);

  /** Returns expected records for updating a field in a nested non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenNestedUdtField(int pk);

  /** Returns expected records for updating a map field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithMapField(int pk);

  /** Returns expected records for updating a list field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithListField(int pk);

  /** Returns expected records for updating a set field in a non-frozen UDT. */
  abstract String[] expectedUpdateNonFrozenUdtWithSetField(int pk);

  /** Returns whether frozen UDT updates are expected for this test variant. */
  protected boolean expectFrozenUdtUpdates() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected void createTypesBeforeTable(String keyspaceName) {
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".simple_udt (a int, b text);");
    // Inner UDT for nested UDT test
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".inner_udt (x int, y text);");
    // Nested UDT containing another UDT
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    // UDT with map
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_map (m frozen<map<text, int>>);");
    // UDT with list
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_list (l frozen<list<int>>);");
    // UDT with set
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_set (s frozen<set<text>>);");
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

  /** Verifies an INSERT with UDT columns emits expected records. */
  @Test
  void testInsert() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
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
    String[] expected = expectedInsert(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies a DELETE emits expected records for UDT columns. */
  @Test
  void testDelete() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
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
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk + ";");
    String[] expected = expectedDelete(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies updates to frozen UDTs from value to value. */
  @Test
  void testUpdateFrozenUdtFromValueToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
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
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
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
            + "WHERE id = "
            + pk
            + ";");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromValueToValue(pk);
      waitAndAssert(pk, expected);
    } else {
      String[] expected = expectedInsertWithFrozenUdt(pk);
      waitAndAssert(pk, expected);
    }
  }

  /** Verifies updates to frozen UDTs from value to null. */
  @Test
  void testUpdateFrozenUdtFromValueToNull() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
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
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
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
            + "WHERE id = "
            + pk
            + ";");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromValueToNull(pk);
      waitAndAssert(pk, expected);
    } else {
      String[] expected = expectedInsertWithFrozenUdt(pk);
      waitAndAssert(pk, expected);
    }
  }

  /** Verifies updates to a field within a non-frozen UDT. */
  @Test
  void testUpdateNonFrozenUdtField() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
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
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
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
            + "WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateNonFrozenUdtField(pk);
    waitAndAssert(pk, expected);
  }
}
