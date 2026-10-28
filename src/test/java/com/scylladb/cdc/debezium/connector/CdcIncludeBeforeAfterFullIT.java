package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.before=full and cdc.include.after=full (default mode).
 *
 * <p>Tests verify that for all operations, the before and after structs contain all columns
 * (complete row state) based on available images.
 *
 * <p>Expected behavior by operation type:
 *
 * <ul>
 *   <li>INSERT: before=null, after=full image (all columns + PK)
 *   <li>UPDATE: before=full preimage (all columns + PK), after=full postimage (all columns + PK)
 *   <li>DELETE: before=full preimage on Scylla >= 2026.1.0, null on older versions; after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla sends a preimage for such deletes only from
 * 2026.1.0, so on older versions the "before" field is null for DELETE operations.
 *
 * <p>The "full" mode provides complete row state in each message, which is useful for consumers
 * that need to know the entire row context.
 */
public class CdcIncludeBeforeAfterFullIT extends CdcIncludeBeforeAfterBase<String, String> {

  private static final String BEFORE_MODE = "full";
  private static final String AFTER_MODE = "full";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, BEFORE_MODE, AFTER_MODE);
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
    // For DELETE on partition-key-only tables before Scylla 2026.1.0, before/after are null.
    // Extract from the "key" field instead.
    return extractIdFromKeyField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  /** INSERT: before=null, after=full image with all columns. */
  @Override
  String[] expectedInsert(int pk) {
    return new String[] {buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())};
  }

  /**
   * DELETE: before=full preimage on Scylla >= 2026.1.0, null on older versions; after=null.
   *
   * <p>This table has only a partition key (no clustering key), so DELETE operations are
   * represented as PARTITION_DELETE by Scylla CDC. Scylla sends a preimage for such deletes only
   * from 2026.1.0. The "key" field contains the PK values for identification.
   */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // DELETE record: before depends on Scylla version, see buildDeleteRecord
      buildDeleteRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // Tombstone record (null value) for Kafka log compaction
      null
    };
  }

  /**
   * TTL DELETE: INSERT record + DELETE record (ROW_DELETE with preimage) + tombstone.
   *
   * @see <a href="https://github.com/scylladb/scylladb/issues/8380">scylladb/scylladb#8380</a>
   */
  @Override
  String[] expectedTtlDelete(int pk) {
    return new String[] {
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      buildTtlDeleteRecord(pk, expectedSource()),
      null
    };
  }

  /**
   * UPDATE (partial - only some primitives modified): before and after contain ALL columns.
   *
   * <p>In "full" mode, even when only a few columns are modified, both before and after contain all
   * columns (complete row state).
   */
  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record - before has Set1 full image, after has partial Set2 + unchanged columns
      buildUpdateRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }

  /**
   * UPDATE (all active columns modified): before and after contain ALL columns.
   *
   * <p>When all active columns are modified, both structs contain all columns - before with Set1
   * values (including untouched), after with Set2 active values and unchanged untouched values.
   */
  @Override
  String[] expectedUpdateMultiColumn(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record - before has full Set1 image, after has full Set2 image
      buildUpdateMultiColumnRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }
}
