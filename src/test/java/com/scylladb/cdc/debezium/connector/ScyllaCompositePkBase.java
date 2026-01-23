package com.scylladb.cdc.debezium.connector;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for composite primary key replication.
 *
 * <p>Each test uses a unique pk1 value (obtained via {@link #reservePk()}) for isolation, allowing
 * all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class ScyllaCompositePkBase<K, V> extends ScyllaTypesIT<K, V> {
  protected static final String PK2_VALUE = "alpha";
  protected static final String PK3_VALUE = "11111111-1111-1111-1111-111111111111";
  protected static final int PK4_VALUE = 10;

  abstract String[] expectedInsert(int pk1);

  abstract String[] expectedUpdate(int pk1);

  abstract String[] expectedDelete(int pk1);

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
  void testInsertWithMultiColumnPk() {
    int pk1 = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + pk1
            + ", '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", "
            + PK4_VALUE
            + ", 'first', 100);");
    waitAndAssert(pk1, expectedInsert(pk1));
  }

  @Test
  void testUpdateWithMultiColumnPk() {
    int pk1 = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + pk1
            + ", '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", "
            + PK4_VALUE
            + ", 'first', 100);");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET value_text = 'second', value_int = 200 WHERE pk1 = "
            + pk1
            + " AND pk2 = '"
            + PK2_VALUE
            + "' AND pk3 = "
            + PK3_VALUE
            + " AND pk4 = "
            + PK4_VALUE
            + ";");
    waitAndAssert(pk1, expectedUpdate(pk1));
  }

  @Test
  void testDeleteWithMultiColumnPk() {
    int pk1 = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (pk1, pk2, pk3, pk4, value_text, value_int) VALUES ("
            + pk1
            + ", '"
            + PK2_VALUE
            + "', "
            + PK3_VALUE
            + ", "
            + PK4_VALUE
            + ", 'first', 100);");
    session.execute(
        "DELETE FROM "
            + getSuiteKeyspaceTableName()
            + " WHERE pk1 = "
            + pk1
            + " AND pk2 = '"
            + PK2_VALUE
            + "' AND pk3 = "
            + PK3_VALUE
            + " AND pk4 = "
            + PK4_VALUE
            + ";");
    waitAndAssert(pk1, expectedDelete(pk1));
  }
}
