package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for complex types (nested UDTs and collections) replication. Tests insert,
 * update, and delete operations with complex nested structures.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesComplexBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String KEYSPACE = "complex_types_ks";

  abstract String[] expectedInsertWithAllTypes();

  abstract String[] expectedInsertWithNullTypes();

  abstract String[] expectedDelete();

  abstract String[] expectedUpdateFrozenAddr();

  abstract String[] expectedUpdateNonFrozenAddrField();

  abstract String[] expectedUpdateNonFrozenAddrSet();

  abstract String[] expectedUpdateNonFrozenAddrMap();

  @Override
  protected String keyspace() {
    return KEYSPACE;
  }

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        + "frozen_addr frozen<address_udt>,"
        + "nf_addr address_udt,"
        + "frozen_addr_list frozen<list<frozen<address_udt>>>,"
        + "nf_addr_set set<frozen<address_udt>>,"
        + "nf_addr_map map<int, frozen<address_udt>>"
        + ")";
  }

  @BeforeAll
  protected static void setup() {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + KEYSPACE
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + KEYSPACE
            + ".address_udt (street text, phones frozen<list<text>>, tags frozen<set<text>>);");
  }

  @Test
  void testInsertWithAllTypes() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_addr, nf_addr, frozen_addr_list, nf_addr_set, nf_addr_map) VALUES ("
            + "1, "
            + "{street: 'main', phones: ['111','222'], tags: {'home','primary'}}, "
            + "{street: 'side', phones: ['333'], tags: {'secondary'}}, "
            + "[{street: 'l1', phones: ['444'], tags: {'list1'}}], "
            + "{{street: 's1', phones: ['666'], tags: {'tag1'}}}, "
            + "{10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}}"
            + ");");
    String[] expected = expectedInsertWithAllTypes();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNullTypes() {
    truncateTables();
    session.execute("INSERT INTO " + tableName() + " (id) VALUES (1);");
    String[] expected = expectedInsertWithNullTypes();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_addr) VALUES (1, {street: 'main', phones: ['111'], tags: {'home'}});");
    session.execute("DELETE FROM " + tableName() + " WHERE id = 1;");
    String[] expected = expectedDelete();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenAddr() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, frozen_addr) VALUES (1, {street: 'main', phones: ['111'], tags: {'home'}});");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET frozen_addr = {street: 'updated', phones: ['999'], tags: {'new'}} WHERE id = 1;");
    String[] expected = expectedUpdateFrozenAddr();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrField() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, nf_addr) VALUES (1, {street: 'side', phones: ['333'], tags: {'secondary'}});");
    session.execute("UPDATE " + tableName() + " SET nf_addr.street = 'modified' WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrField();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrSet() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, nf_addr_set) VALUES (1, {{street: 's1', phones: ['666'], tags: {'tag1'}}});");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET nf_addr_set = nf_addr_set + {{street: 's2', phones: ['777'], tags: {'tag2'}}} WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrSet();
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrMap() {
    truncateTables();
    session.execute(
        "INSERT INTO "
            + tableName()
            + " (id, nf_addr_map) VALUES (1, {10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}});");
    session.execute(
        "UPDATE "
            + tableName()
            + " SET nf_addr_map = nf_addr_map + {20: {street: 'm2', phones: ['999'], tags: {'tagm2'}}} WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrMap();
    waitAndAssert(getConsumer(), expected);
  }
}
