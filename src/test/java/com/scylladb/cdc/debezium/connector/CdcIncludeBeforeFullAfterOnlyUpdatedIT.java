package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for mixed mode: cdc.include.before=full and cdc.include.after=only-updated.
 *
 * <p>Tests verify that before and after modes can be configured independently:
 *
 * <ul>
 *   <li>before: always includes all columns (full row state)
 *   <li>after: includes only modified columns for UPDATE operations
 * </ul>
 *
 * <p>Expected behavior by operation type:
 *
 * <ul>
 *   <li>INSERT: before=null, after=full image (INSERT always uses full image)
 *   <li>UPDATE: before=full preimage (all columns), after=only modified columns + PK
 *   <li>DELETE: before=full preimage on Scylla >= 2026.1.0, null on older versions; after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla sends a preimage for such deletes only from
 * 2026.1.0, so on older versions the "before" field is null for DELETE operations.
 *
 * <p>This mixed mode is useful when consumers need complete state for before (e.g., for auditing)
 * but want minimal data for after (e.g., for efficient change propagation).
 */
public class CdcIncludeBeforeFullAfterOnlyUpdatedIT
    extends CdcIncludeBeforeAfterBase<String, String> {

  private static final String BEFORE_MODE = "full";
  private static final String AFTER_MODE = "only-updated";

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

  /**
   * INSERT: before=null, after=full image with all columns.
   *
   * <p>INSERT always uses full image for after, regardless of the only-updated setting.
   */
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
   * UPDATE (partial): before=full image, after=only modified columns + PK.
   *
   * <p>Mixed mode: before contains ALL columns (full state), but after contains only the modified
   * columns (untouched columns excluded).
   */
  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record: before=full, after=only-updated
      buildUpdateRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }

  /**
   * UPDATE (all active columns): before=full image, after=all modified columns + PK.
   *
   * <p>When all active columns are modified, the after struct will contain all of them, but
   * untouched columns are still excluded from after.
   */
  @Override
  String[] expectedUpdateMultiColumn(int pk) {
    return new String[] {
      // INSERT record with full image
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record: before=full, after=only-updated (all active)
      buildUpdateMultiColumnRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }
}
