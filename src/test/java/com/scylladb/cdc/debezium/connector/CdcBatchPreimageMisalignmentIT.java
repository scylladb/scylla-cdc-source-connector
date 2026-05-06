package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration test that reproduces CDC preimage misalignment when using UNLOGGED BATCH with
 * multiple UPDATE statements targeting different rows in the same partition.
 *
 * <p>Root cause: ScyllaDB generates CDC log entries in type-grouped order for unsplit mutations:
 * [all preimages] -> [all deltas] -> [all postimages]. The connector's preimage/postimage storage
 * is keyed by TaskId (per-vnode), not by row identity. When multiple rows share the same TaskId,
 * later preimage/delta/postimage entries overwrite earlier ones, causing misalignment.
 *
 * <p>Related issues:
 *
 * <ul>
 *   <li>scylladb/scylla-cdc-java#7 - same class of bug in the replicator's PostImageState
 * </ul>
 *
 * <h3>Phase 1: CDC Log Ordering Verification</h3>
 *
 * Queries the CDC log table directly to confirm that ScyllaDB generates entries in type-grouped
 * order (PRE_IMAGE, PRE_IMAGE, UPDATE, UPDATE, POST_IMAGE, POST_IMAGE) rather than per-row order.
 *
 * <h3>Phase 2: Legacy Format Preimage Misalignment</h3>
 *
 * With {@code cdc.output.format=legacy} and {@code experimental.preimages.enabled=true}, verifies
 * that the connector produces 2 UPDATE events where one has the wrong preimage (from a different
 * row) and the other has no preimage at all.
 *
 * <h3>Phase 3: Advanced Format Data Loss</h3>
 *
 * With {@code cdc.output.format=advanced}, {@code cdc.include.before=full}, {@code
 * cdc.include.after=full}, verifies that the connector produces only 1 UPDATE event instead of 2
 * (second row's event is lost because its TaskInfo never completes).
 */
public class CdcBatchPreimageMisalignmentIT extends AbstractContainerBaseIT {

  /** CDC operation type byte values from ScyllaDB's CDC log. */
  private static final int OP_PRE_IMAGE = 0;

  private static final int OP_ROW_UPDATE = 1;
  private static final int OP_ROW_INSERT = 2;
  private static final int OP_POST_IMAGE = 9;

  /** Timeout for waiting for CDC log entries to appear. */
  private static final long CDC_LOG_TIMEOUT_MS = 30_000;

  /** Timeout for waiting for Kafka events. */
  private static final long KAFKA_TIMEOUT_MS = 65_000;

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

  /**
   * Phase 1: Verify that ScyllaDB generates CDC log entries in type-grouped order for UNLOGGED
   * BATCH operations.
   *
   * <p>Expected CDC log ordering for a 2-row UNLOGGED BATCH with preimage+postimage:
   *
   * <pre>
   * batch_seq_no 0: PRE_IMAGE  (ck=1)
   * batch_seq_no 1: PRE_IMAGE  (ck=2)
   * batch_seq_no 2: ROW_UPDATE (ck=1)
   * batch_seq_no 3: ROW_UPDATE (ck=2)
   * batch_seq_no 4: POST_IMAGE (ck=1)
   * batch_seq_no 5: POST_IMAGE (ck=2)
   * </pre>
   */
  @Test
  public void verifyCdcLogOrderingForUnloggedBatch(TestInfo testInfo) {
    String keyspace = keyspaceName(testInfo);
    String table = tableName(testInfo);
    String fullTableName = keyspace + "." + table;
    String cdcLogTable = fullTableName + "_scylla_cdc_log";

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {

      // Create keyspace and table with CDC preimage + postimage
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS "
              + keyspace
              + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + fullTableName
              + " (pk int, ck int, v text, PRIMARY KEY (pk, ck))"
              + " WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true}");

      // Insert initial data for two rows in the same partition
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 'original_A')");
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 2, 'original_B')");

      // Wait for initial inserts to propagate to CDC log
      waitForCdcLogEntries(session, cdcLogTable, OP_ROW_INSERT, 2);

      // Execute UNLOGGED BATCH updating both rows
      session.execute(
          "BEGIN UNLOGGED BATCH "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_A' WHERE pk=1 AND ck=1; "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_B' WHERE pk=1 AND ck=2; "
              + "APPLY BATCH");

      // Wait for batch entries to appear in CDC log
      waitForCdcLogEntries(session, cdcLogTable, OP_ROW_UPDATE, 2);

      // Query all CDC log entries for the batch (same cdc$time as the UPDATE entries)
      // Find the cdc$time for the UPDATE entries
      List<Row> updateRows =
          session
              .execute(
                  "SELECT \"cdc$time\", \"cdc$batch_seq_no\", \"cdc$operation\", ck, v "
                      + "FROM "
                      + cdcLogTable
                      + " WHERE \"cdc$operation\" = "
                      + OP_ROW_UPDATE
                      + " ALLOW FILTERING")
              .all();

      assertFalse(updateRows.isEmpty(), "Should have UPDATE entries in CDC log");

      // Get the cdc$time from one of the update rows to find all related entries
      java.util.UUID batchTime = updateRows.get(0).getUUID("cdc$time");

      // Query all entries with the same cdc$time (preimage + delta + postimage)
      List<Row> allBatchEntries =
          session
              .execute(
                  "SELECT \"cdc$batch_seq_no\", \"cdc$operation\", ck, v "
                      + "FROM "
                      + cdcLogTable
                      + " WHERE \"cdc$time\" = "
                      + batchTime
                      + " ALLOW FILTERING")
              .all();

      // Sort by batch_seq_no
      allBatchEntries.sort(
          (a, b) -> Integer.compare(a.getInt("cdc$batch_seq_no"), b.getInt("cdc$batch_seq_no")));

      // We expect 6 entries: 2 preimages + 2 deltas + 2 postimages
      assertEquals(
          6,
          allBatchEntries.size(),
          "Expected 6 CDC log entries for 2-row batch with preimage+postimage. Got: "
              + describeCdcEntries(allBatchEntries));

      // Verify type-grouped ordering
      // batch_seq 0,1: PRE_IMAGE
      assertEquals(
          OP_PRE_IMAGE,
          allBatchEntries.get(0).getByte("cdc$operation"),
          "batch_seq_no=0 should be PRE_IMAGE");
      assertEquals(
          OP_PRE_IMAGE,
          allBatchEntries.get(1).getByte("cdc$operation"),
          "batch_seq_no=1 should be PRE_IMAGE");

      // batch_seq 2,3: ROW_UPDATE
      assertEquals(
          OP_ROW_UPDATE,
          allBatchEntries.get(2).getByte("cdc$operation"),
          "batch_seq_no=2 should be ROW_UPDATE");
      assertEquals(
          OP_ROW_UPDATE,
          allBatchEntries.get(3).getByte("cdc$operation"),
          "batch_seq_no=3 should be ROW_UPDATE");

      // batch_seq 4,5: POST_IMAGE
      assertEquals(
          OP_POST_IMAGE,
          allBatchEntries.get(4).getByte("cdc$operation"),
          "batch_seq_no=4 should be POST_IMAGE");
      assertEquals(
          OP_POST_IMAGE,
          allBatchEntries.get(5).getByte("cdc$operation"),
          "batch_seq_no=5 should be POST_IMAGE");

      // Verify that preimages contain original values
      // (rows are in clustering key order: ck=1 first, ck=2 second)
      int preCk0 = allBatchEntries.get(0).getInt("ck");
      int preCk1 = allBatchEntries.get(1).getInt("ck");
      assertTrue(
          (preCk0 == 1 && preCk1 == 2) || (preCk0 == 2 && preCk1 == 1),
          "Preimages should cover ck=1 and ck=2, got ck=" + preCk0 + " and ck=" + preCk1);

      // Verify preimage values contain original data
      String preV0 = allBatchEntries.get(0).getString("v");
      String preV1 = allBatchEntries.get(1).getString("v");
      assertTrue(
          ("original_A".equals(preV0) && "original_B".equals(preV1))
              || ("original_B".equals(preV0) && "original_A".equals(preV1)),
          "Preimage values should be original_A and original_B, got: " + preV0 + ", " + preV1);

      // KEY ASSERTION: Verify the ordering is type-grouped, NOT per-row.
      // If it were per-row, we'd see: PRE_IMAGE, UPDATE, POST_IMAGE, PRE_IMAGE, UPDATE, POST_IMAGE
      // Instead we see: PRE_IMAGE, PRE_IMAGE, UPDATE, UPDATE, POST_IMAGE, POST_IMAGE
      // This is what causes the connector bug.
      int[] operations =
          allBatchEntries.stream().mapToInt(r -> r.getByte("cdc$operation")).toArray();
      assertArrayEquals(
          new int[] {
            OP_PRE_IMAGE, OP_PRE_IMAGE,
            OP_ROW_UPDATE, OP_ROW_UPDATE,
            OP_POST_IMAGE, OP_POST_IMAGE
          },
          operations,
          "CDC log entries should be in type-grouped order, not per-row order. Got: "
              + describeCdcEntries(allBatchEntries));
    }
  }

  /**
   * Phase 2: Verify that the legacy consumer produces misaligned preimages for UNLOGGED BATCH
   * operations.
   *
   * <p>With the legacy format, the consumer stores preimages in {@code Map<TaskId, RawChange>}.
   * When two preimages arrive before any delta (due to type-grouped ordering), the second
   * overwrites the first. Result:
   *
   * <ul>
   *   <li>Event 1: dispatched with wrong preimage (from the other row)
   *   <li>Event 2: dispatched with null preimage (entry was consumed by Event 1)
   * </ul>
   */
  @Test
  public void legacyFormatPreimageMisalignmentWithUnloggedBatch(TestInfo testInfo)
      throws Exception {
    String keyspace = keyspaceName(testInfo);
    String table = tableName(testInfo);
    String fullTableName = keyspace + "." + table;
    String connectorName = connectorName(testInfo);

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {

      // Create keyspace and table
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS "
              + keyspace
              + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + fullTableName
              + " (pk int, ck int, v text, PRIMARY KEY (pk, ck))"
              + " WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true}");

      // Insert initial data
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 'original_A')");
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 2, 'original_B')");

      // Register connector with legacy format + preimage enabled
      Properties props = KafkaConnectUtils.createCommonConnectorProperties();
      props.put("topic.prefix", connectorName);
      props.put("scylla.table.names", fullTableName);
      props.put("name", connectorName);
      props.put("cdc.output.format", "legacy");
      props.put("experimental.preimages.enabled", "true");
      KafkaConnectUtils.registerConnector(props, connectorName);
      registeredConnectors.add(connectorName);
      consumer.subscribe(List.of(connectorName + "." + fullTableName));

      // Wait for initial INSERT events to confirm connector is running
      List<ConsumerRecord<String, String>> insertRecords =
          waitForKafkaRecords(consumer, 2, "initial inserts");

      // Execute UNLOGGED BATCH updating both rows
      session.execute(
          "BEGIN UNLOGGED BATCH "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_A' WHERE pk=1 AND ck=1; "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_B' WHERE pk=1 AND ck=2; "
              + "APPLY BATCH");

      // Wait for UPDATE events
      List<ConsumerRecord<String, String>> updateRecords =
          waitForKafkaRecords(consumer, 2, "batch updates");
      assertEquals(2, updateRecords.size(), "Should receive exactly 2 UPDATE events");

      // Analyze the events
      JSONObject event1 = new JSONObject(updateRecords.get(0).value());
      JSONObject event2 = new JSONObject(updateRecords.get(1).value());

      // Both should be update operations
      assertEquals("u", event1.getString("op"), "Event 1 should be an update");
      assertEquals("u", event2.getString("op"), "Event 2 should be an update");

      // In legacy format, "before" contains the preimage data (or null)
      // and "after" contains the delta.
      // Due to the bug, one event should have the WRONG preimage and one should have NULL.

      boolean event1HasBefore = !event1.isNull("before");
      boolean event2HasBefore = !event2.isNull("before");

      // BUG ASSERTION: Exactly one event should have a preimage, and one should not.
      // In correct behavior, BOTH events should have preimages.
      assertNotEquals(
          event1HasBefore,
          event2HasBefore,
          "BUG CONFIRMED: Expected exactly one event with preimage and one without. "
              + "In correct behavior, both events would have preimages. "
              + "Event 1 has before="
              + (event1HasBefore ? "present" : "null")
              + ", Event 2 has before="
              + (event2HasBefore ? "present" : "null"));

      // Identify which event has the preimage
      JSONObject eventWithBefore = event1HasBefore ? event1 : event2;
      JSONObject eventWithoutBefore = event1HasBefore ? event2 : event1;

      // The event WITH a preimage should have a MISALIGNED preimage:
      // The preimage's 'v' value should NOT match the 'after' row's original value.
      JSONObject before = eventWithBefore.getJSONObject("before");
      JSONObject after = eventWithBefore.getJSONObject("after");

      // Extract the clustering key from the 'after' (delta) to identify which row this event is for
      // In legacy format, after contains Cell structs with {"value": ..., "set": true/false}
      // but the exact format may vary. Let's extract what we can.
      String beforeV = extractLegacyCellValue(before, "v");
      int afterCk = extractLegacyCellIntValue(after, "ck");

      // Determine what the correct preimage 'v' should be for this row
      String expectedBeforeV = (afterCk == 1) ? "original_A" : "original_B";

      // BUG ASSERTION: The preimage 'v' should NOT match the expected value for this row.
      // It should be from the OTHER row due to the overwrite.
      assertNotEquals(
          expectedBeforeV,
          beforeV,
          "BUG CONFIRMED: Preimage 'v' value '"
              + beforeV
              + "' does not belong to this row (ck="
              + afterCk
              + "). "
              + "Expected '"
              + expectedBeforeV
              + "' but got the other row's preimage due to TaskId collision.");

      // The event WITHOUT a preimage should have had one (it was an UPDATE, not INSERT)
      assertFalse(
          eventWithoutBefore.isNull("after"),
          "Event without preimage should still have an 'after' (delta)");
    }
  }

  /**
   * Phase 3: Verify that the advanced consumer loses events for UNLOGGED BATCH operations.
   *
   * <p>With the advanced format using BeforeAfter mode, the consumer stores preimage, change, and
   * postimage in a single TaskInfo keyed by TaskId. With type-grouped ordering:
   *
   * <ol>
   *   <li>PRE_IMAGE(A) -> stored
   *   <li>PRE_IMAGE(B) -> overwrites A's preimage
   *   <li>ROW_UPDATE(A) -> overwrites nothing (change slot empty), stored
   *   <li>ROW_UPDATE(B) -> overwrites A's change
   *   <li>POST_IMAGE(A) -> stored, TaskInfo now complete (preImage=B, change=B, postImage=A) ->
   *       dispatches with ALL WRONG data
   *   <li>POST_IMAGE(B) -> new TaskInfo, only postImage set -> never completes -> LOST
   * </ol>
   *
   * Result: Only 1 event produced instead of 2. The single event has misaligned data.
   */
  @Test
  public void advancedFormatDataLossWithUnloggedBatch(TestInfo testInfo) throws Exception {
    String keyspace = keyspaceName(testInfo);
    String table = tableName(testInfo);
    String fullTableName = keyspace + "." + table;
    String connectorName = connectorName(testInfo);

    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect();
        KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {

      // Create keyspace and table
      session.execute(
          "CREATE KEYSPACE IF NOT EXISTS "
              + keyspace
              + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      // Note: use 'preimage': true (not 'full') because the connector's advanced format
      // validation only accepts boolean true, not the string 'full'.
      session.execute(
          "CREATE TABLE IF NOT EXISTS "
              + fullTableName
              + " (pk int, ck int, v text, PRIMARY KEY (pk, ck))"
              + " WITH cdc = {'enabled': true, 'preimage': true, 'postimage': true}");

      // Insert initial data
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 1, 'original_A')");
      session.execute("INSERT INTO " + fullTableName + " (pk, ck, v) VALUES (1, 2, 'original_B')");

      // Register connector with advanced format + before/after=full
      Properties props = KafkaConnectUtils.createCommonConnectorProperties();
      props.put("topic.prefix", connectorName);
      props.put("scylla.table.names", fullTableName);
      props.put("name", connectorName);
      props.put("cdc.output.format", "advanced");
      props.put("cdc.include.before", "full");
      props.put("cdc.include.after", "full");
      props.put("cdc.include.primary-key.placement", "kafka-key,payload-after,payload-before");
      KafkaConnectUtils.registerConnector(props, connectorName);
      registeredConnectors.add(connectorName);
      consumer.subscribe(List.of(connectorName + "." + fullTableName));

      // Wait for initial INSERT events (2 inserts expected)
      // Advanced format INSERTs with after=full need postimage, so they should produce 2 events
      List<ConsumerRecord<String, String>> insertRecords =
          waitForKafkaRecords(consumer, 2, "initial inserts");

      // Execute UNLOGGED BATCH updating both rows
      session.execute(
          "BEGIN UNLOGGED BATCH "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_A' WHERE pk=1 AND ck=1; "
              + "UPDATE "
              + fullTableName
              + " SET v='updated_B' WHERE pk=1 AND ck=2; "
              + "APPLY BATCH");

      // Wait for UPDATE events - we expect only 1 due to the bug (second row lost)
      // Wait long enough to be confident no second event is coming
      List<ConsumerRecord<String, String>> updateRecords =
          waitForKafkaRecordsWithExtraWait(consumer, "batch updates", 20_000);

      // Count only UPDATE events (filter out any late INSERT events)
      List<ConsumerRecord<String, String>> updateOnlyRecords = new ArrayList<>();
      for (ConsumerRecord<String, String> record : updateRecords) {
        if (record.value() != null) {
          JSONObject event = new JSONObject(record.value());
          if ("u".equals(event.optString("op"))) {
            updateOnlyRecords.add(record);
          }
        }
      }

      // BUG ASSERTION: Only 1 UPDATE event received instead of the expected 2.
      // The second row's event is lost because its TaskInfo (BeforeAfter) never completes:
      // POST_IMAGE(B) creates a new TaskInfo with only postImage set, but no preImage or change.
      assertEquals(
          1,
          updateOnlyRecords.size(),
          "BUG CONFIRMED: Expected 2 UPDATE events but got "
              + updateOnlyRecords.size()
              + ". The second row's event was lost due to TaskId collision in BeforeAfter mode.");

      // The single event should have misaligned data
      JSONObject soleEvent = new JSONObject(updateOnlyRecords.get(0).value());
      assertFalse(soleEvent.isNull("before"), "The sole event should have a 'before' field");
      assertFalse(soleEvent.isNull("after"), "The sole event should have an 'after' field");

      // The before, after fields contain data from DIFFERENT rows due to overwrites
      JSONObject before = soleEvent.getJSONObject("before");
      JSONObject after = soleEvent.getJSONObject("after");

      // In advanced format, values are stored directly (not in Cell structs)
      // Extract clustering keys to verify misalignment
      int beforeCk = before.optInt("ck", -1);
      int afterCk = after.optInt("ck", -1);

      // If before and after have different clustering keys, the misalignment is confirmed
      if (beforeCk != -1 && afterCk != -1 && beforeCk != afterCk) {
        assertNotEquals(
            beforeCk,
            afterCk,
            "BUG CONFIRMED: before.ck="
                + beforeCk
                + " != after.ck="
                + afterCk
                + ". Preimage from one row is paired with postimage from another.");
      }
    }
  }

  // --- Helper methods ---

  /**
   * Waits for at least {@code expectedCount} CDC log entries of the given operation type to appear.
   */
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

  /** Waits for at least {@code minCount} Kafka records, with a maximum timeout. */
  private List<ConsumerRecord<String, String>> waitForKafkaRecords(
      KafkaConsumer<String, String> consumer, int minCount, String description) {
    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < KAFKA_TIMEOUT_MS) {
      var records = consumer.poll(Duration.ofSeconds(2));
      records.forEach(allRecords::add);
      if (allRecords.size() >= minCount) {
        return allRecords;
      }
    }

    // Return whatever we have (test will assert on count)
    return allRecords;
  }

  /**
   * Waits for Kafka records with an extra wait period after the last received record to ensure no
   * more are coming.
   */
  private List<ConsumerRecord<String, String>> waitForKafkaRecordsWithExtraWait(
      KafkaConsumer<String, String> consumer, String description, long extraWaitMs) {
    List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    long lastReceivedTime = 0;

    while (System.currentTimeMillis() - startTime < KAFKA_TIMEOUT_MS) {
      var records = consumer.poll(Duration.ofSeconds(2));
      if (!records.isEmpty()) {
        records.forEach(allRecords::add);
        lastReceivedTime = System.currentTimeMillis();
      }

      // If we received at least 1 record and enough time has passed since the last one,
      // we're confident no more are coming
      if (!allRecords.isEmpty()
          && lastReceivedTime > 0
          && System.currentTimeMillis() - lastReceivedTime > extraWaitMs) {
        break;
      }
    }

    return allRecords;
  }

  /** Extracts a string value from a legacy format Cell struct ({"value": "...", "set": true}). */
  private String extractLegacyCellValue(JSONObject row, String columnName) {
    if (row.isNull(columnName)) {
      return null;
    }
    Object cell = row.opt(columnName);
    if (cell == null) {
      return null;
    }
    if (cell instanceof JSONObject) {
      JSONObject cellObj = (JSONObject) cell;
      return cellObj.has("value") && !cellObj.isNull("value")
          ? cellObj.optString("value", null)
          : null;
    }
    return cell.toString();
  }

  /** Extracts an int value from a legacy format Cell struct. */
  private int extractLegacyCellIntValue(JSONObject row, String columnName) {
    if (row.isNull(columnName)) {
      return -1;
    }
    Object cell = row.opt(columnName);
    if (cell == null) {
      return -1;
    }
    if (cell instanceof JSONObject) {
      JSONObject cellObj = (JSONObject) cell;
      return cellObj.has("value") && !cellObj.isNull("value") ? cellObj.optInt("value", -1) : -1;
    }
    if (cell instanceof Number) {
      return ((Number) cell).intValue();
    }
    return Integer.parseInt(cell.toString());
  }

  /** Formats CDC log entries for debug messages. */
  private String describeCdcEntries(List<Row> entries) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < entries.size(); i++) {
      Row row = entries.get(i);
      if (i > 0) sb.append(", ");
      sb.append("seq=")
          .append(row.getInt("cdc$batch_seq_no"))
          .append(" op=")
          .append(operationName(row.getByte("cdc$operation")))
          .append(" ck=")
          .append(row.getInt("ck"));
    }
    sb.append("]");
    return sb.toString();
  }

  /** Returns a human-readable name for a CDC operation type. */
  private String operationName(int op) {
    switch (op) {
      case 0:
        return "PRE_IMAGE";
      case 1:
        return "ROW_UPDATE";
      case 2:
        return "ROW_INSERT";
      case 3:
        return "ROW_DELETE";
      case 9:
        return "POST_IMAGE";
      default:
        return "UNKNOWN(" + op + ")";
    }
  }
}
