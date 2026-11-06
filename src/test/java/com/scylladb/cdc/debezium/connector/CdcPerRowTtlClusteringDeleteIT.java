package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Per-row TTL expiry on a table with a clustering key, which ScyllaDB logs as a row-scoped service
 * delete ({@code cdc$operation} -3). {@link CdcPerRowTtlDeleteIT} covers the partition-key-only
 * table, whose expiry is logged as a service partition delete (-4).
 *
 * <p>Records are keyed by {@code (id, ck)}; they are filtered by {@code id}, so all records of one
 * partition are asserted together, in log order: the IT topics have the broker's default single
 * partition, which keeps the order across keys.
 */
public class CdcPerRowTtlClusteringDeleteIT extends ScyllaTypesIT<String, String> {

  @BeforeAll
  @Override
  public void setupSuite(TestInfo testInfo) {
    Assumptions.assumeTrue(
        PARSED_SCYLLA_VERSION != null
            && PARSED_SCYLLA_VERSION.isAtLeast(ScyllaVersion.PER_ROW_TTL_SUPPORT),
        "Per-row TTL tests require ScyllaDB >= " + ScyllaVersion.PER_ROW_TTL_SUPPORT);
    super.setupSuite(testInfo);
  }

  @Override
  protected String createTableCql(String tableName) {
    return "(id int, ck int, text_col text, expiration timestamp TTL, PRIMARY KEY (id, ck))";
  }

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties props = KafkaConnectUtils.createCommonConnectorProperties();
    props.put("topic.prefix", connectorName);
    props.put("scylla.table.names", tableName);
    props.put("name", connectorName);
    props.put("cdc.output.format", "advanced");
    props.put("cdc.include.before", "full");
    props.put("cdc.include.after", "full");
    props.put(
        "cdc.include.primary-key.placement", "kafka-key,payload-after,payload-before,payload-key");
    KafkaConnectUtils.registerConnector(props, connectorName);
    consumer.subscribe(List.of(connectorName + "." + tableName));
    return consumer;
  }

  @Override
  protected int extractPkFromValue(String value) {
    int pk = extractIdFromAfter(value);
    if (pk != -1) {
      return pk;
    }
    pk = extractIdFromBefore(value);
    if (pk != -1) {
      return pk;
    }
    return extractIdFromKeyField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  private void insert(int pk, int ck, String text, String expirationCql) {
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, ck, text_col, expiration) VALUES ("
            + pk
            + ", "
            + ck
            + ", '"
            + text
            + "', "
            + expirationCql
            + ")");
  }

  private static String key(int pk, int ck) {
    return "{\"id\": " + pk + ", \"ck\": " + ck + "}";
  }

  private static String row(int pk, int ck, String text, String expirationJson) {
    return "{\"id\": "
        + pk
        + ", \"ck\": "
        + ck
        + ", \"text_col\": \""
        + text
        + "\", \"expiration\": "
        + expirationJson
        + "}";
  }

  /**
   * An expired row yields INSERT, a DELETE whose before is the row including ck, and a tombstone.
   */
  @Test
  void testPerRowTtlDelete() throws InterruptedException {
    int pk = reservePk();
    long expirationMs = System.currentTimeMillis() + 5000;
    insert(pk, 0, "per_row_ttl_test", "'" + Instant.ofEpochMilli(expirationMs) + "'");

    // The expiry scan runs every alternator_ttl_period_in_seconds (1 s in ITs).
    Thread.sleep(15000);

    String rowData = row(pk, 0, "per_row_ttl_test", String.valueOf(expirationMs));
    waitAndAssert(
        pk,
        new String[] {
          expectedRecord("c", "null", rowData, key(pk, 0)),
          expectedRecord("d", rowData, "null", key(pk, 0)),
          null
        });
  }

  /**
   * Two rows of one partition, only one expiring: only that row gets a DELETE, and the other row
   * none.
   */
  @Test
  void testPerRowTtlDeleteLeavesOtherRowsOfThePartition() throws InterruptedException {
    int pk = reservePk();
    long expirationMs = System.currentTimeMillis() + 5000;
    insert(pk, 0, "expiring", "'" + Instant.ofEpochMilli(expirationMs) + "'");
    insert(pk, 1, "not_expiring", "null");

    Thread.sleep(15000);

    String expiring = row(pk, 0, "expiring", String.valueOf(expirationMs));
    // After the tombstone, collect for one 1 s TTL scan plus the time a spurious DELETE of ck=1
    // would take to be consumed.
    waitAndAssertNoMore(
        pk,
        new String[] {
          expectedRecord("c", "null", expiring, key(pk, 0)),
          expectedRecord("c", "null", row(pk, 1, "not_expiring", "null"), key(pk, 1)),
          expectedRecord("d", expiring, "null", key(pk, 0)),
          null
        },
        1000 + KafkaConnectUtils.CDC_QUIET_PERIOD_MS);
  }
}
