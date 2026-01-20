package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public abstract class ScyllaCompositePkBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String PK2_VALUE = "alpha";
  protected static final String PK3_VALUE = "11111111-1111-1111-1111-111111111111";

  abstract String[] expectedInsert(TestInfo testInfo);

  abstract String[] expectedUpdate(TestInfo testInfo);

  abstract String[] expectedDelete(TestInfo testInfo);

  @BeforeAll
  static void setupKeyspace(TestInfo testInfo) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspaceName(testInfo)
            + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
  }

  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "pk1 int,"
        + "pk2 text,"
        + "pk3 uuid,"
        + "pk4 int,"
        + "value_text text,"
        + "value_int int,"
        + "PRIMARY KEY ((pk1, pk2), pk3, pk4)"
        + ")";
  }

  @Test
  void testInsertWithMultiColumnPk(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + "1, '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", 10, 'first', 100);");
    waitAndAssert(getConsumer(), expectedInsert(testInfo));
  }

  @Test
  void testUpdateWithMultiColumnPk(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + "1, '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", 10, 'first', 100);");
    session.execute(
        "UPDATE "
            + keyspaceTableName(testInfo)
            + " SET value_text = 'second', value_int = 200 WHERE pk1 = 1 AND pk2 = '"
            + PK2_VALUE
            + "' AND pk3 = "
            + PK3_VALUE
            + " AND pk4 = 10;");
    waitAndAssert(getConsumer(), expectedUpdate(testInfo));
  }

  @Test
  void testDeleteWithMultiColumnPk(TestInfo testInfo) {
    truncateTables(testInfo);
    session.execute(
        "INSERT INTO "
            + keyspaceTableName(testInfo)
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + "1, '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", 10, 'first', 100);");
    session.execute(
        "DELETE FROM "
            + keyspaceTableName(testInfo)
            + " WHERE pk1 = 1 AND pk2 = '"
            + PK2_VALUE
            + "' AND pk3 = "
            + PK3_VALUE
            + " AND pk4 = 10;");
    waitAndAssert(getConsumer(), expectedDelete(testInfo));
  }
}
