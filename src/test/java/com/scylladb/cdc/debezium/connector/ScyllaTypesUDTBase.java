package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for UDT (User-Defined Types) replication. Tests insert, update, and delete
 * operations with both frozen and non-frozen UDTs.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesUDTBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String KEYSPACE = "udt_ks";

  abstract String[] expectedInsertWithFrozenUdt();

  abstract String[] expectedInsertWithNonFrozenUdt();

  abstract String[] expectedInsertWithNullUdt();

  abstract String[] expectedDelete();

  abstract String[] expectedUpdateFrozenUdtFromValueToValue();

  abstract String[] expectedUpdateFrozenUdtFromValueToNull();

  abstract String[] expectedUpdateNonFrozenUdtField();

  @Override
  protected String keyspace() {
    return KEYSPACE;
  }

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "frozen_udt frozen<simple_udt>,"
        + "nf_udt simple_udt"
        + ")";
  }

  @BeforeAll
  protected static void setup() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + KEYSPACE
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute("CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".simple_udt (a int, b text);");
  }

  @Test
  void testInsertWithFrozenUdt() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    String[] expected = expectedInsertWithFrozenUdt();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNonFrozenUdt() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, null, {a: 7, b: 'bar'});");
    String[] expected = expectedInsertWithNonFrozenUdt();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNullUdt() {
    truncateTables();
    session.execute(
        "INSERT INTO " + tableName() + " (id, frozen_udt, nf_udt) VALUES (1, null, null);");
    String[] expected = expectedInsertWithNullUdt();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, {a: 7, b: 'bar'});");
    session.execute("DELETE FROM " + tableName() + " WHERE id = 1;");
    String[] expected = expectedDelete();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenUdtFromValueToValue() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    session.execute(
        "UPDATE " + tableName() + " SET frozen_udt = {a: 99, b: 'updated'} WHERE id = 1;");
    String[] expected = expectedUpdateFrozenUdtFromValueToValue();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenUdtFromValueToNull() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, {a: 42, b: 'foo'}, null);");
    session.execute("UPDATE " + tableName() + " SET frozen_udt = null WHERE id = 1;");
    String[] expected = expectedUpdateFrozenUdtFromValueToNull();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenUdtField() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_udt, nf_udt) VALUES (1, null, {a: 7, b: 'bar'});");
    session.execute("UPDATE " + tableName() + " SET nf_udt.a = 100 WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenUdtField();
    waitAndAssert(getConsumer(), expected);
  }
}
