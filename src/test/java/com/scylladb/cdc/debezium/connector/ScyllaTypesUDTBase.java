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

  abstract String[] expectedInsertWithFrozenUdt(TestInfo testInfo);

  abstract String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo);

  abstract String[] expectedInsertWithNullUdt(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo);

  abstract String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo);

  abstract String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo);

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "frozen_udt frozen<simple_udt>,"
        + "nf_udt simple_udt"
        + ")";
  }

  @BeforeAll
  protected static void setup(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName(testInfo) + ".simple_udt (a int, b text);");
  }

  @Test
  void testInsertWithFrozenUdt(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    String[] expected = expectedInsertWithFrozenUdt(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNonFrozenUdt(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, null, {a: 7, b: 'bar'});");
    String[] expected = expectedInsertWithNonFrozenUdt(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNullUdt(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, null, null);");
    String[] expected = expectedInsertWithNullUdt(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, {a: 7, b: 'bar'});");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_udt = {a: 99, b: 'updated'} WHERE id = 1;");
    String[] expected = expectedUpdateFrozenUdtFromValueToValue(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET frozen_udt = null WHERE id = 1;");
    String[] expected = expectedUpdateFrozenUdtFromValueToNull(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenUdtField(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_udt, nf_udt) VALUES (1, null, {a: 7, b: 'bar'});");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET nf_udt.a = 100 WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenUdtField(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
