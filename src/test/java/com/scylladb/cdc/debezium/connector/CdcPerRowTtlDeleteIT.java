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
 * Integration tests for CDC records generated when rows expire due to per-row TTL.
 *
 * <p>Per-row TTL is a ScyllaDB feature (not present in Cassandra) where a column is designated as
 * the TTL column using {@code timestamp TTL} syntax. Unlike per-write TTL ({@code USING TTL N}),
 * per-row TTL uses an active background thread to delete expired rows, which guarantees that CDC
 * DELETE events are generated (including preimage if enabled).
 *
 * <p>Table schema uses {@code expiration timestamp TTL} to designate the expiration column:
 *
 * <pre>{@code
 * CREATE TABLE t (
 *   id int PRIMARY KEY,
 *   text_col text,
 *   expiration timestamp TTL
 * ) WITH cdc = {'enabled':true, 'preimage':true, 'postimage':true}
 * }</pre>
 *
 * <p>This test verifies:
 *
 * <ul>
 *   <li>INSERT with a near-future expiration followed by background deletion produces INSERT +
 *       DELETE CDC records
 *   <li>INSERT with null expiration (no TTL) does not expire
 *   <li>Explicit DELETE of a row with per-row TTL produces normal DELETE CDC records
 * </ul>
 *
 * @see <a href="https://github.com/scylladb/scylladb/pull/28320">scylladb/scylladb#28320</a>
 *     Per-row TTL implementation
 * @see <a href="https://github.com/scylladb/scylladb/issues/13000">scylladb/scylladb#13000</a>
 *     Per-row TTL feature request
 * @see CdcIncludeBeforeAfterBase#testTtlDelete() for per-write TTL tests across before/after modes
 */
public class CdcPerRowTtlDeleteIT extends ScyllaTypesIT<String, String> {

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
    return "(id int PRIMARY KEY, text_col text, expiration timestamp TTL)";
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

  /**
   * Tests that a row with per-row TTL expiration generates a CDC DELETE record.
   *
   * <p>Per-row TTL uses an active background deletion thread (same as Alternator TTL). When the row
   * expires and is deleted by the background thread, a CDC DELETE event is generated with a
   * preimage.
   *
   * <p>Expected sequence:
   *
   * <ol>
   *   <li>INSERT record (op="c") with full after image including the expiration timestamp
   *   <li>DELETE record (op="d") with before=preimage (the row data before deletion), after=null
   *   <li>Tombstone record (null value) for Kafka log compaction
   * </ol>
   *
   * <p>Note: The deletion timing depends on {@code alternator_ttl_period_in_seconds}. The test uses
   * a generous timeout to account for the background scan interval.
   */
  @Test
  void testPerRowTtlDelete() throws InterruptedException {
    int pk = reservePk();

    // Set expiration 5 seconds in the future
    long expirationMs = System.currentTimeMillis() + 5000;
    String expirationCql = Instant.ofEpochMilli(expirationMs).toString();

    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, text_col, expiration) VALUES ("
            + pk
            + ", 'per_row_ttl_test', '"
            + expirationCql
            + "')");

    // Wait for the background deletion thread to scan and delete the expired row.
    // The scan interval is controlled by alternator_ttl_period_in_seconds (default varies).
    Thread.sleep(15000);

    String key = "{\"id\": " + pk + "}";
    String rowData =
        "{\"id\": "
            + pk
            + ", \"text_col\": \"per_row_ttl_test\", \"expiration\": "
            + expirationMs
            + "}";

    waitAndAssert(
        pk,
        new String[] {
          // 1. INSERT record with full after image (includes expiration timestamp)
          expectedRecord("c", "null", rowData, key),
          // 2. DELETE record from per-row TTL expiry (active deletion by background thread):
          //    before = preimage with the row data that existed before deletion
          //    after  = null (row no longer exists)
          expectedRecord("d", rowData, "null", key),
          // 3. Tombstone for Kafka log compaction
          null
        });
  }

  /**
   * Tests that an explicit DELETE of a row with per-row TTL set produces normal DELETE CDC records.
   *
   * <p>When a row with per-row TTL is explicitly deleted before expiry, the explicit DELETE takes
   * precedence and the background thread will not generate a second DELETE.
   */
  @Test
  void testExplicitDeleteBeforePerRowTtlExpiry() {
    int pk = reservePk();

    // Set expiration far in the future so it won't expire during the test
    long expirationMs = System.currentTimeMillis() + 300000;
    String expirationCql = Instant.ofEpochMilli(expirationMs).toString();

    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, text_col, expiration) VALUES ("
            + pk
            + ", 'explicit_delete_test', '"
            + expirationCql
            + "')");

    // Explicitly delete the row before the per-row TTL expires
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk);

    String key = "{\"id\": " + pk + "}";

    waitAndAssert(
        pk,
        new String[] {
          // 1. INSERT record
          expectedRecord(
              "c",
              "null",
              "{\"id\": "
                  + pk
                  + ", \"text_col\": \"explicit_delete_test\", \"expiration\": "
                  + expirationMs
                  + "}",
              key),
          // 2. Explicit DELETE record (PARTITION_DELETE — no preimage available)
          expectedRecord("d", "null", "null", key),
          // 3. Tombstone for Kafka log compaction
          null
        });
  }

  /**
   * Tests that a row with null expiration (no per-row TTL) is not deleted by the background thread.
   *
   * <p>When the expiration column is null, the row has no per-row TTL and should persist
   * indefinitely. Only the INSERT record should appear.
   */
  @Test
  void testPerRowTtlNullExpiration() throws InterruptedException {
    int pk = reservePk();

    // Insert with null expiration — row should not expire
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, text_col, expiration) VALUES ("
            + pk
            + ", 'no_ttl_test', null)");

    // Wait to confirm no DELETE is generated
    Thread.sleep(10000);

    String key = "{\"id\": " + pk + "}";

    // Only the INSERT record should be produced — null expiration means no TTL
    waitAndAssert(
        pk,
        new String[] {
          expectedRecord("c", "null", "{\"id\": " + pk + ", \"text_col\": \"no_ttl_test\"}", key),
        });
  }
}
