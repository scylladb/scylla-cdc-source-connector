package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for complex types (nested UDTs and collections) replication. Tests insert,
 * update, and delete operations with complex nested structures.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaTypesComplexBase<K, V> extends ScyllaTypesIT<K, V> {

  abstract String[] expectedInsertWithAllTypes(TestInfo testInfo);

  abstract String[] expectedInsertWithNullTypes(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  abstract String[] expectedUpdateFrozenAddr(TestInfo testInfo);

  abstract String[] expectedUpdateNonFrozenAddrField(TestInfo testInfo);

  abstract String[] expectedUpdateNonFrozenAddrSet(TestInfo testInfo);

  abstract String[] expectedUpdateNonFrozenAddrMap(TestInfo testInfo);

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
  protected static void setup(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + ".address_udt (street text, phones frozen<list<text>>, tags frozen<set<text>>);");
  }

  @Test
  void testInsertWithAllTypes(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_addr, nf_addr, frozen_addr_list, nf_addr_set, nf_addr_map) VALUES ("
            + "1, "
            + "{street: 'main', phones: ['111','222'], tags: {'home','primary'}}, "
            + "{street: 'side', phones: ['333'], tags: {'secondary'}}, "
            + "[{street: 'l1', phones: ['444'], tags: {'list1'}}], "
            + "{{street: 's1', phones: ['666'], tags: {'tag1'}}}, "
            + "{10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}}"
            + ");");
    String[] expected = expectedInsertWithAllTypes(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testInsertWithNullTypes(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute("INSERT INTO " + keyspaceTableName(testInfo) + " (id) VALUES (1);");
    String[] expected = expectedInsertWithNullTypes(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testDelete(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_addr) VALUES (1, {street: 'main', phones: ['111'], tags: {'home'}});");
    session.execute("DELETE FROM " + keyspaceTableName(testInfo) + " WHERE id = 1;");
    String[] expected = expectedDelete(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateFrozenAddr(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, frozen_addr) VALUES (1, {street: 'main', phones: ['111'], tags: {'home'}});");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET frozen_addr = {street: 'updated', phones: ['999'], tags: {'new'}} WHERE id = 1;");
    String[] expected = expectedUpdateFrozenAddr(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrField(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, nf_addr) VALUES (1, {street: 'side', phones: ['333'], tags: {'secondary'}});");
    session.execute(
        "UPDATE " + keyspaceTableName(testInfo) + " SET nf_addr.street = 'modified' WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrField(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrSet(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, nf_addr_set) VALUES (1, {{street: 's1', phones: ['666'], tags: {'tag1'}}});");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET nf_addr_set = nf_addr_set + {{street: 's2', phones: ['777'], tags: {'tag2'}}} WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrSet(testInfo);
    waitAndAssert(getConsumer(), expected);
  }

  @Test
  void testUpdateNonFrozenAddrMap(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (id, nf_addr_map) VALUES (1, {10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}});");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET nf_addr_map = nf_addr_map + {20: {street: 'm2', phones: ['999'], tags: {'tagm2'}}} WHERE id = 1;");
    String[] expected = expectedUpdateNonFrozenAddrMap(testInfo);
    waitAndAssert(getConsumer(), expected);
  }
}
