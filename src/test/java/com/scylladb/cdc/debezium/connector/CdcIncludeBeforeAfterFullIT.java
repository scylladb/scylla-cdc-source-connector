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
 *   <li>DELETE: before=null (Scylla doesn't send preimage for PARTITION_DELETE), after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
 * deletes, so the "before" field will be null for DELETE operations.
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
    // For DELETE on partition-key-only tables, before/after are null.
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
   * DELETE: before=null (partition delete has no preimage), after=null.
   *
   * <p>This table has only a partition key (no clustering key), so DELETE operations are
   * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
   * deletes, so "before" is null. The "key" field contains the PK values for identification.
   */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // DELETE record: before=null because Scylla doesn't send preimage for PARTITION_DELETE
      buildDeleteRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // Tombstone record (null value) for Kafka log compaction
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
