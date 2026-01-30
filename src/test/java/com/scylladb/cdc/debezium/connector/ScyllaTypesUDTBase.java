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

  /** Returns expected records for inserting a row with all UDT columns set to null. */
  abstract String[] expectedInsertWithNull(int pk);

  /** Returns expected records for inserting a row with UDTs having all fields null (empty UDTs). */
  abstract String[] expectedInsertWithEmpty(int pk);

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

  /** Returns expected records for updating frozen UDTs from value to empty (all fields null). */
  abstract String[] expectedUpdateFrozenUdtFromValueToEmpty(int pk);

  /** Returns expected records for updating frozen UDTs from null to populated value. */
  abstract String[] expectedUpdateFrozenUdtFromNullToValue(int pk);

  /**
   * Returns expected records for updating frozen UDTs from empty (all fields null) to populated
   * value.
   */
  abstract String[] expectedUpdateFrozenUdtFromEmptyToValue(int pk);

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

  /** Returns expected records for replacing entire non-frozen UDTs with new values. */
  abstract String[] expectedUpdateNonFrozenUdtFromValueToValue(int pk);

  /** Returns expected records for setting entire non-frozen UDTs to null. */
  abstract String[] expectedUpdateNonFrozenUdtFromValueToNull(int pk);

  /** Returns whether frozen UDT updates are expected for this test variant. */
  protected boolean expectFrozenUdtUpdates() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected void createTypesBeforeTable(String keyspaceName) {
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".simple_udt (a int, b text);");
    // Inner UDT for nested UDT test
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".inner_udt (x int, y text);");
    // Nested UDT containing another UDT
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    // UDT with map (using timeuuid key for nested collection coverage)
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_map (m frozen<map<timeuuid, text>>);");
    // UDT with list
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_list (l frozen<list<int>>);");
    // UDT with set
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_set (s frozen<set<text>>);");
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    String[] expected = expectedInsert(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies an INSERT with all UDT columns set to null. */
  @Test
  void testInsertWithNull() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
            + "null, null, null, null, null, null, null, null, null, null"
            + ");");
    String[] expected = expectedInsertWithNull(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies an INSERT with UDTs having all fields null (empty UDTs). */
  @Test
  void testInsertWithEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
            + "{a: null, b: null}, "
            + "{a: null, b: null}, "
            + "{inner: null, z: null}, "
            + "{inner: null, z: null}, "
            + "{m: null}, "
            + "{m: null}, "
            + "{l: null}, "
            + "{l: null}, "
            + "{s: null}, "
            + "{s: null}"
            + ");");
    String[] expected = expectedInsertWithEmpty(pk);
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
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
            + "{a: 42, b: 'foo'}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{inner: {x: 10, y: 'hello'}, z: 20}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
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
            + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}}, "
            + "nf_udt_with_map.m = {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}, "
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
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
            + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}}, "
            + "nf_udt_with_map.m = {81d4a030-4632-11f0-9484-409dd8f36eba: 'updated1', 81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}, "
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

  /** Verifies updates to frozen UDTs from value to empty (all fields null). */
  @Test
  void testUpdateFrozenUdtFromValueToEmpty() {
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET frozen_udt = {a: null, b: null}, "
            + "nf_udt = {a: null, b: null}, "
            + "frozen_nested_udt = {inner: null, z: null}, "
            + "nf_nested_udt = {inner: null, z: null}, "
            + "frozen_udt_with_map = {m: null}, "
            + "nf_udt_with_map = {m: null}, "
            + "frozen_udt_with_list = {l: null}, "
            + "nf_udt_with_list = {l: null}, "
            + "frozen_udt_with_set = {s: null}, "
            + "nf_udt_with_set = {s: null} "
            + "WHERE id = "
            + pk
            + ";");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromValueToEmpty(pk);
      waitAndAssert(pk, expected);
    } else {
      String[] expected = expectedInsert(pk);
      waitAndAssert(pk, expected);
    }
  }

  /** Verifies updates to frozen UDTs from null to populated value. */
  @Test
  void testUpdateFrozenUdtFromNullToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
            + "null, null, null, null, null, null, null, null, null, null"
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET frozen_udt = {a: 42, b: 'foo'}, "
            + "nf_udt = {a: 7, b: 'bar'}, "
            + "frozen_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
            + "nf_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
            + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "nf_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "frozen_udt_with_list = {l: [1, 2, 3]}, "
            + "nf_udt_with_list = {l: [1, 2, 3]}, "
            + "frozen_udt_with_set = {s: {'a', 'b', 'c'}}, "
            + "nf_udt_with_set = {s: {'a', 'b', 'c'}} "
            + "WHERE id = "
            + pk
            + ";");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromNullToValue(pk);
      waitAndAssert(pk, expected);
    } else {
      String[] expected = expectedInsertWithNull(pk);
      waitAndAssert(pk, expected);
    }
  }

  /** Verifies updates to frozen UDTs from empty (all fields null) to populated value. */
  @Test
  void testUpdateFrozenUdtFromEmptyToValue() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, "
            + "frozen_udt_with_map, nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, "
            + "frozen_udt_with_set, nf_udt_with_set) VALUES ("
            + pk
            + ", "
            + "{a: null, b: null}, "
            + "{a: null, b: null}, "
            + "{inner: null, z: null}, "
            + "{inner: null, z: null}, "
            + "{m: null}, "
            + "{m: null}, "
            + "{l: null}, "
            + "{l: null}, "
            + "{s: null}, "
            + "{s: null}"
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET frozen_udt = {a: 42, b: 'foo'}, "
            + "nf_udt = {a: 7, b: 'bar'}, "
            + "frozen_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
            + "nf_nested_udt = {inner: {x: 10, y: 'hello'}, z: 20}, "
            + "frozen_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "nf_udt_with_map = {m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "frozen_udt_with_list = {l: [1, 2, 3]}, "
            + "nf_udt_with_list = {l: [1, 2, 3]}, "
            + "frozen_udt_with_set = {s: {'a', 'b', 'c'}}, "
            + "nf_udt_with_set = {s: {'a', 'b', 'c'}} "
            + "WHERE id = "
            + pk
            + ";");
    if (expectFrozenUdtUpdates()) {
      String[] expected = expectedUpdateFrozenUdtFromEmptyToValue(pk);
      waitAndAssert(pk, expected);
    } else {
      String[] expected = expectedInsertWithEmpty(pk);
      waitAndAssert(pk, expected);
    }
  }

  /** Verifies replacing entire non-frozen UDTs with new values. */
  @Test
  void testUpdateNonFrozenUdtFromValueToValue() {
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET nf_udt = {a: 99, b: 'updated'}, "
            + "nf_nested_udt = {inner: {x: 11, y: 'updated'}, z: 21}, "
            + "nf_udt_with_map = {m: {81d4a032-4632-11f0-9484-409dd8f36eba: 'value3', 81d4a033-4632-11f0-9484-409dd8f36eba: 'value4'}}, "
            + "nf_udt_with_list = {l: [4, 5, 6]}, "
            + "nf_udt_with_set = {s: {'d', 'e', 'f'}} "
            + "WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateNonFrozenUdtFromValueToValue(pk);
    waitAndAssert(pk, expected);
  }

  /** Verifies setting entire non-frozen UDTs to null. */
  @Test
  void testUpdateNonFrozenUdtFromValueToNull() {
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
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
            + "{l: [1, 2, 3]}, "
            + "{l: [1, 2, 3]}, "
            + "{s: {'a', 'b', 'c'}}, "
            + "{s: {'a', 'b', 'c'}}"
            + ");");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET nf_udt = null, "
            + "nf_nested_udt = null, "
            + "nf_udt_with_map = null, "
            + "nf_udt_with_list = null, "
            + "nf_udt_with_set = null "
            + "WHERE id = "
            + pk
            + ";");
    String[] expected = expectedUpdateNonFrozenUdtFromValueToNull(pk);
    waitAndAssert(pk, expected);
  }
}
