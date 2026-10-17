package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Regression tests for the CDC log split caused by writing a STATIC column together with clustered
 * columns in a single statement.
 *
 * <p>ScyllaDB stores static columns in a separate <em>static row</em> within the partition. A
 * single CQL statement that writes a static column <b>and</b> clustered columns is recorded as
 * <b>two</b> CDC log entries sharing one {@code cdc$time}, distinguished by {@code
 * cdc$batch_seq_no}, with {@code cdc$end_of_batch} set only on the last. The connector emits one
 * Kafka record per CDC log entry and performs no merging by {@code cdc$time}, so one statement
 * produces two data change events.
 *
 * <p>The static entry carries a <b>null clustering key</b> and only the static column. Because key
 * columns are optional, its Kafka message key is {@code {pk, ck: null}} — a different key from the
 * row's other events — so a sink that upserts by key materialises a separate row.
 *
 * <p>Observed CDC log for an INSERT (clustered columns only) followed by an UPDATE that touches the
 * static column as well:
 *
 * <pre>
 * seq 0, end_of_batch=true  ROW_INSERT  ck=0     v
 * seq 0, end_of_batch=null  ROW_UPDATE  ck=null  s only      &lt;-- the split static entry
 * seq 1, end_of_batch=true  ROW_UPDATE  ck=0     w
 * </pre>
 *
 * <p>Reproduced on ScyllaDB 6.2, 2025.1.9, 2025.4.0 and 2026.2.4 with {@code cdc = {'enabled':
 * true}} — preimage/postimage are not required.
 *
 * <p>Note the keyspaces here disable tablets: ScyllaDB 2025.1.x refuses to create a CDC log for a
 * table in a tablet keyspace (scylladb/scylladb#16317).
 *
 * @see <a href="https://github.com/scylladb/scylla-cdc-source-connector/issues/341">#341</a>
 * @see <a href="https://github.com/scylladb/scylla-cdc-source-connector/issues/342">#342</a>
 */
public class CdcStaticColumnSplitIT extends AbstractContainerBaseIT {

  /** CDC operation type byte values from ScyllaDB's CDC log. */
  private static final int OP_ROW_UPDATE = 1;

  private static final int OP_ROW_INSERT = 2;

  private static final long CDC_LOG_TIMEOUT_MS = 30_000;
  private static final long KAFKA_TIMEOUT_MS = 65_000;

  /** Quiet period used to be confident no further records are in flight. */
  private static final long QUIET_PERIOD_MS = 15_000;

  private final List<String> registeredConnectors = new ArrayList<>();

  @AfterEach
  public void cleanUp() {
    for (String connectorName : registeredConnectors) {
      try {
        KafkaConnectUtils.removeConnector(connectorName);
      } catch (Exception e) {
        // Ignore cleanup errors
      }
    }
    registeredConnectors.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 1 — the CDC log itself, with no connector involved.
  // Cheapest and first-failing signal: if ScyllaDB stops splitting, this fails with a clear
  // message before any Kafka assertion has a chance to confuse the diagnosis.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void cdcLogSplitsStaticWriteIntoSeparateEntry(TestInfo testInfo) {
    String keyspace = keyspaceName(testInfo);
    String fullTableName = keyspace + "." + tableName(testInfo);
    String cdcLogTable = fullTableName + "_scylla_cdc_log";

    try (Cluster cluster = newCluster();
        Session session = cluster.connect()) {

      createKeyspace(session, keyspace);
      createTable(session, fullTableName, true);

      insertClusteredOnly(session, fullTableName, 1);
      waitForCdcLogEntries(session, cdcLogTable, OP_ROW_INSERT, 1);
      updateTouchingStatic(session, fullTableName, 1);
      waitForCdcLogEntries(session, cdcLogTable, OP_ROW_UPDATE, 2);

      List<Row> rows = session.execute("SELECT * FROM " + cdcLogTable + " ALLOW FILTERING").all();
      assertEquals(3, rows.size(), "two statements should produce three CDC log entries");

      // Group by cdc$time: the INSERT alone, and the UPDATE split in two.
      List<List<Row>> groups =
          new ArrayList<>(
              rows.stream()
                  .collect(Collectors.groupingBy(r -> r.getUUID("cdc$time").toString()))
                  .values());
      groups.sort(Comparator.comparingInt(List::size));

      assertEquals(2, groups.size(), "entries should fall into two cdc$time groups");
      assertEquals(1, groups.get(0).size(), "the INSERT should be a single entry");
      assertEquals(2, groups.get(1).size(), "the UPDATE should be split into two entries");

      // Identify the halves by content, not by cdc$batch_seq_no: ScyllaDB does not guarantee which
      // of them comes first within the batch.
      // NOTE: Row.getInt returns 0 for a NULL column, so null clustering keys must be
      // tested with isNull, never getInt.
      List<Row> updateGroup = groups.get(1);
      Row staticEntry =
          updateGroup.stream()
              .filter(r -> r.isNull("ck"))
              .findFirst()
              .orElseGet(() -> fail("the split static entry must have a null clustering key"));
      Row clusteredEntry =
          updateGroup.stream()
              .filter(r -> !r.isNull("ck"))
              .findFirst()
              .orElseGet(() -> fail("the clustered entry must keep its clustering key"));
      assertEquals("static1", staticEntry.getString("s"), "static entry carries the static column");
      assertTrue(staticEntry.isNull("w"), "static entry carries nothing else");
      assertEquals(OP_ROW_UPDATE, staticEntry.getByte("cdc$operation"));

      assertEquals(0, clusteredEntry.getInt("ck"), "the clustered entry keeps its clustering key");
      assertEquals("w1", clusteredEntry.getString("w"));
      assertTrue(clusteredEntry.isNull("s"), "the static value lives only in the split entry");

      // end_of_batch marks the last entry of the batch only, whichever half that is.
      List<Row> bySeqNo = new ArrayList<>(updateGroup);
      bySeqNo.sort(Comparator.comparingInt(r -> r.getInt("cdc$batch_seq_no")));
      Row first = bySeqNo.get(0);
      Row last = bySeqNo.get(1);
      assertTrue(
          first.getInt("cdc$batch_seq_no") != last.getInt("cdc$batch_seq_no"),
          "the two halves must have distinct cdc$batch_seq_no");
      assertTrue(
          first.isNull("cdc$end_of_batch") || !first.getBool("cdc$end_of_batch"),
          "end_of_batch must not be set on the first entry of the batch");
      assertTrue(
          last.getBool("cdc$end_of_batch"),
          "end_of_batch must be set on the last entry of the batch");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 2 — Kafka records, legacy format.
  //
  // Legacy is the primary config because it matches the measured scenario exactly: the table needs
  // only {'enabled': true}, and legacy builds `after` straight from the CDC delta, so the record
  // shape can be asserted. The advanced format sources `after` from a postimage, which would
  // require changing the table's CDC options and therefore the scenario under test.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void staticColumnWriteEmitsExtraKafkaRecord(TestInfo testInfo) throws Exception {
    String keyspace = keyspaceName(testInfo);
    String fullTableName = keyspace + "." + tableName(testInfo);
    String connectorName = connectorName(testInfo);

    try (Cluster cluster = newCluster();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {

      createKeyspace(session, keyspace);
      createTable(session, fullTableName, true);

      Properties props = KafkaConnectUtils.createCommonConnectorProperties();
      props.put("topic.prefix", connectorName);
      props.put("scylla.table.names", fullTableName);
      props.put("name", connectorName);
      props.put("cdc.output.format", "legacy");
      // experimental.preimages.enabled deliberately unset: no grouping, one record per CDC entry.
      KafkaConnectUtils.registerConnector(props, connectorName);
      registeredConnectors.add(connectorName);

      consumer.subscribe(List.of(connectorName + "." + fullTableName));
      KafkaConnectUtils.waitForConsumerAssignment(consumer);

      // pk=1: UPDATE touches the static column -> expect 3 records.
      insertClusteredOnly(session, fullTableName, 1);
      updateTouchingStatic(session, fullTableName, 1);
      // pk=2 (negative control): UPDATE touches clustered columns only -> expect 2 records.
      insertClusteredOnly(session, fullTableName, 2);
      updateClusteredOnly(session, fullTableName, 2);
      // pk=3: a single INSERT touching the static column -> expect 2 records.
      insertTouchingStatic(session, fullTableName, 3);

      List<ConsumerRecord<String, String>> records =
          waitForKafkaRecords(consumer, 7, QUIET_PERIOD_MS);

      List<ConsumerRecord<String, String>> pk1Records = recordsForPk(records, 1);
      List<JSONObject> pk1 = values(pk1Records);
      List<JSONObject> pk2 = values(recordsForPk(records, 2));
      List<JSONObject> pk3 = values(recordsForPk(records, 3));

      assertEquals(
          3,
          pk1.size(),
          "a statement touching a static column alongside clustered columns yields an extra record");
      assertEquals(
          2, pk2.size(), "an UPDATE touching only clustered columns yields no extra record");

      List<JSONObject> staticEvents =
          pk1.stream().filter(e -> hasNullCk(afterOf(e))).collect(Collectors.toList());
      assertEquals(1, staticEvents.size(), "exactly one record should carry a null clustering key");

      JSONObject staticAfter = afterOf(staticEvents.get(0));
      assertEquals("u", staticEvents.get(0).optString("op", null));
      assertEquals(1, staticAfter.optInt("pk", -1));
      assertEquals("static1", legacyCellValue(staticAfter, "s"), "static column is populated");
      assertNull(legacyCellValue(staticAfter, "v"), "no other column is populated");
      assertNull(legacyCellValue(staticAfter, "w"), "no other column is populated");

      JSONObject clusteredUpdate =
          pk1.stream()
              .filter(e -> !hasNullCk(afterOf(e)) && "u".equals(e.optString("op", null)))
              .findFirst()
              .orElseGet(() -> fail("expected a clustered UPDATE record"));
      assertEquals("w1", legacyCellValue(afterOf(clusteredUpdate), "w"));
      assertNull(
          legacyCellValue(afterOf(clusteredUpdate), "s"),
          "the static value appears only in the split record, not in the clustered one");

      // Both halves of one statement share cdc$time, so they share source.ts_us. The INSERT,
      // being a different statement, does not. This is order-independent, which matters because
      // the differing message keys may place the records in different topic partitions.
      long staticTsUs = staticEvents.get(0).optJSONObject("source").optLong("ts_us", -1L);
      long clusteredTsUs = clusteredUpdate.optJSONObject("source").optLong("ts_us", -1L);
      assertEquals(
          staticTsUs, clusteredTsUs, "both records came from one statement, so ts_us must match");

      JSONObject insertEvent =
          pk1.stream()
              .filter(e -> "c".equals(e.optString("op", null)))
              .findFirst()
              .orElseGet(() -> fail("expected an INSERT record"));
      assertTrue(
          insertEvent.optJSONObject("source").optLong("ts_us", -1L) != staticTsUs,
          "the INSERT is a separate statement and must have a different ts_us");

      // The message key of the split record differs from the row's other records, which is why an
      // upsert sink materialises a separate row for it.
      ConsumerRecord<String, String> staticRecord =
          pk1Records.stream()
              .filter(r -> hasNullCk(afterOf(json(r.value()))))
              .findFirst()
              .orElseGet(() -> fail("could not locate the static record"));
      assertStaticKey(staticRecord, 1);

      // An INSERT writing the static column splits too; the static half is an update, because the
      // static row has no row marker.
      assertEquals(2, pk3.size(), "an INSERT touching a static column yields two records");
      JSONObject staticInsert =
          pk3.stream()
              .filter(e -> hasNullCk(afterOf(e)))
              .findFirst()
              .orElseGet(() -> fail("expected a static record for the INSERT"));
      assertEquals("u", staticInsert.optString("op", null));
      assertEquals("static3", legacyCellValue(afterOf(staticInsert), "s"));
      JSONObject clusteredInsert =
          pk3.stream()
              .filter(e -> !hasNullCk(afterOf(e)))
              .findFirst()
              .orElseGet(() -> fail("expected a clustered record for the INSERT"));
      assertEquals("c", clusteredInsert.optString("op", null));
      assertEquals("v3", legacyCellValue(afterOf(clusteredInsert), "v"));
    }
  }

  @Test
  public void nonStaticColumnEmitsNoExtraRecord(TestInfo testInfo) throws Exception {
    String keyspace = keyspaceName(testInfo);
    String fullTableName = keyspace + "." + tableName(testInfo);
    String connectorName = connectorName(testInfo);

    try (Cluster cluster = newCluster();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {

      createKeyspace(session, keyspace);
      createTable(session, fullTableName, false); // identical table, column NOT static

      Properties props = KafkaConnectUtils.createCommonConnectorProperties();
      props.put("topic.prefix", connectorName);
      props.put("scylla.table.names", fullTableName);
      props.put("name", connectorName);
      props.put("cdc.output.format", "legacy");
      KafkaConnectUtils.registerConnector(props, connectorName);
      registeredConnectors.add(connectorName);

      consumer.subscribe(List.of(connectorName + "." + fullTableName));
      KafkaConnectUtils.waitForConsumerAssignment(consumer);

      insertClusteredOnly(session, fullTableName, 1);
      updateTouchingStatic(session, fullTableName, 1); // same statement shape, non-static column

      List<ConsumerRecord<String, String>> records =
          waitForKafkaRecords(consumer, 2, QUIET_PERIOD_MS);
      List<JSONObject> pk1 = values(recordsForPk(records, 1));

      assertEquals(2, pk1.size(), "without a static column the same statements yield two records");
      assertTrue(
          pk1.stream().noneMatch(e -> hasNullCk(afterOf(e))),
          "no record should carry a null clustering key");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Phase 3 — advanced format, to show the split is not an artefact of the legacy emitter.
  //
  // before=none/after=none keeps the table options unchanged. `payload-key` is required: the
  // default placement puts the primary key in payload-after/payload-before, both of which are null
  // under `none`, leaving nothing identifying in the value.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void staticColumnSplitInAdvancedFormat(TestInfo testInfo) throws Exception {
    String keyspace = keyspaceName(testInfo);
    String fullTableName = keyspace + "." + tableName(testInfo);
    String connectorName = connectorName(testInfo);

    try (Cluster cluster = newCluster();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {

      createKeyspace(session, keyspace);
      createTable(session, fullTableName, true);

      Properties props = KafkaConnectUtils.createCommonConnectorProperties();
      props.put("topic.prefix", connectorName);
      props.put("scylla.table.names", fullTableName);
      props.put("name", connectorName);
      props.put("cdc.output.format", "advanced");
      props.put("cdc.include.before", "none");
      props.put("cdc.include.after", "none");
      props.put("cdc.include.primary-key.placement", "kafka-key,payload-key");
      KafkaConnectUtils.registerConnector(props, connectorName);
      registeredConnectors.add(connectorName);

      consumer.subscribe(List.of(connectorName + "." + fullTableName));
      KafkaConnectUtils.waitForConsumerAssignment(consumer);

      insertClusteredOnly(session, fullTableName, 1);
      updateTouchingStatic(session, fullTableName, 1);

      List<ConsumerRecord<String, String>> records =
          waitForKafkaRecords(consumer, 3, QUIET_PERIOD_MS);

      // before/after are null by design under none/none (see CdcIncludeBeforeAfterNoneIT); the
      // identifying information is in the payload `key` struct and the message key.
      List<ConsumerRecord<String, String>> dataRecords =
          records.stream().filter(r -> r.value() != null).collect(Collectors.toList());
      List<JSONObject> events = values(dataRecords);
      assertEquals(3, events.size(), "two statements should still yield three records");

      List<ConsumerRecord<String, String>> staticRecords =
          dataRecords.stream()
              .filter(r -> hasNullCk(json(r.value()).optJSONObject("key")))
              .collect(Collectors.toList());
      assertEquals(
          1, staticRecords.size(), "exactly one record should carry a null clustering key");
      List<JSONObject> staticEvents = values(staticRecords);
      assertEquals("u", staticEvents.get(0).optString("op", null));
      assertEquals(1, staticEvents.get(0).optJSONObject("key").optInt("pk", -1));
      assertStaticKey(staticRecords.get(0), 1);

      long staticTsUs = staticEvents.get(0).optJSONObject("source").optLong("ts_us", -1L);
      long matching =
          events.stream()
              .filter(e -> e.optJSONObject("source").optLong("ts_us", -1L) == staticTsUs)
              .count();
      assertEquals(
          2,
          matching,
          "the split record shares its statement, and so its ts_us, with exactly one other");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private Cluster newCluster() {
    return Cluster.builder()
        .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
        .withPort(scyllaDBContainer.getMappedPort(9042))
        .build();
  }

  /** ScyllaDB 2025.1.x cannot create a CDC log in a tablet keyspace (scylladb/scylladb#16317). */
  private void createKeyspace(Session session, String keyspace) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS "
            + keyspace
            + " WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            + " AND tablets = {'enabled': false}");
  }

  private void createTable(Session session, String fullTableName, boolean staticColumn) {
    session.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullTableName
            + " (pk int, ck int, s text"
            + (staticColumn ? " STATIC" : "")
            + ", v text, w text, PRIMARY KEY (pk, ck))"
            + " WITH cdc = {'enabled': true}");
  }

  private void insertClusteredOnly(Session session, String fullTableName, int pk) {
    session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (" + pk + ", 0, 'v0')");
  }

  /** Writes the static column and a clustered column in one statement — the split trigger. */
  private void updateTouchingStatic(Session session, String fullTableName, int pk) {
    session.execute(
        "UPDATE " + fullTableName + " SET w='w1', s='static1' WHERE pk=" + pk + " AND ck=0");
  }

  /** Writes the static column and a clustered column in one INSERT. */
  private void insertTouchingStatic(Session session, String fullTableName, int pk) {
    session.execute(
        "INSERT INTO "
            + fullTableName
            + " (pk, ck, s, v) VALUES ("
            + pk
            + ", 0, 'static"
            + pk
            + "', 'v"
            + pk
            + "')");
  }

  private void updateClusteredOnly(Session session, String fullTableName, int pk) {
    session.execute("UPDATE " + fullTableName + " SET w='w1' WHERE pk=" + pk + " AND ck=0");
  }

  private void waitForCdcLogEntries(
      Session session, String cdcLogTable, int operationType, int expectedCount) {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < CDC_LOG_TIMEOUT_MS) {
      List<Row> rows =
          session
              .execute(
                  "SELECT * FROM "
                      + cdcLogTable
                      + " WHERE \"cdc$operation\" = "
                      + operationType
                      + " ALLOW FILTERING")
              .all();
      if (rows.size() >= expectedCount) {
        return;
      }
      Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }
    fail(
        "Timed out waiting for "
            + expectedCount
            + " CDC log entries of operation type "
            + operationType
            + " in "
            + cdcLogTable);
  }

  /**
   * Polls until at least {@code minCount} records have arrived and then stays quiet. Once {@code
   * minCount} is reached the quiet period is always waited out, even past {@code KAFKA_TIMEOUT_MS},
   * so the "no extra record" checks cannot pass on a truncated window.
   */
  private List<ConsumerRecord<String, String>> waitForKafkaRecords(
      KafkaConsumer<String, String> consumer, int minCount, long quietPeriodMs) {
    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    long lastReceivedTime = 0;

    while (System.currentTimeMillis() - startTime < KAFKA_TIMEOUT_MS + quietPeriodMs
        && (allRecords.size() >= minCount
            || System.currentTimeMillis() - startTime < KAFKA_TIMEOUT_MS)) {
      var records = consumer.poll(Duration.ofSeconds(2));
      if (!records.isEmpty()) {
        records.forEach(allRecords::add);
        lastReceivedTime = System.currentTimeMillis();
      }
      if (allRecords.size() >= minCount
          && lastReceivedTime > 0
          && System.currentTimeMillis() - lastReceivedTime > quietPeriodMs) {
        break;
      }
    }
    return allRecords;
  }

  private JSONObject afterOf(JSONObject event) {
    return event.optJSONObject("after");
  }

  /** Buckets records by the partition key in their {@code after} struct. */
  private List<ConsumerRecord<String, String>> recordsForPk(
      List<ConsumerRecord<String, String>> records, int pk) {
    List<ConsumerRecord<String, String>> result = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
      if (record.value() == null) {
        continue; // tombstone
      }
      JSONObject event = json(record.value());
      if (event.isNull("after")) {
        continue;
      }
      JSONObject after = event.optJSONObject("after");
      if (!after.isNull("pk") && after.optInt("pk", -1) == pk) {
        result.add(record);
      }
    }
    return result;
  }

  /**
   * Parses JSON, rethrowing org.json's checked exception unchecked so it can be used in lambdas.
   */
  private static JSONObject json(String text) {
    try {
      return new JSONObject(text);
    } catch (JSONException e) {
      throw new IllegalStateException("not a JSON object: " + text, e);
    }
  }

  private List<JSONObject> values(List<ConsumerRecord<String, String>> records) {
    return records.stream().map(r -> json(r.value())).collect(Collectors.toList());
  }

  /**
   * True only when {@code ck} is present and null; {@code isNull} alone also accepts a missing
   * field.
   */
  private boolean hasNullCk(JSONObject struct) {
    return struct.has("ck") && struct.isNull("ck");
  }

  private void assertStaticKey(ConsumerRecord<String, String> record, int pk) {
    JSONObject key = json(record.key());
    assertEquals(pk, key.optInt("pk", -1));
    assertTrue(hasNullCk(key), "the static record's message key has a null clustering key");
  }

  /** Legacy format wraps non-PK values in a Cell struct: {@code {"value": …}}. */
  private String legacyCellValue(JSONObject after, String columnName) {
    if (after.isNull(columnName)) {
      return null;
    }
    Object cell = after.opt(columnName);
    if (cell instanceof JSONObject) {
      JSONObject cellObj = (JSONObject) cell;
      return cellObj.has("value") && !cellObj.isNull("value")
          ? cellObj.optString("value", null)
          : null;
    }
    return cell.toString();
  }
}
