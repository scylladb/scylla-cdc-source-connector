package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for mixed mode: cdc.include.before=only-updated and cdc.include.after=full.
 *
 * <p>Tests verify that before and after modes can be configured independently:
 *
 * <ul>
 *   <li>before: includes only modified columns for UPDATE operations
 *   <li>after: always includes all columns (full row state)
 * </ul>
 *
 * <p>Expected behavior by operation type:
 *
 * <ul>
 *   <li>INSERT: before=null, after=full image
 *   <li>UPDATE: before=only modified columns + PK, after=full postimage (all columns)
 *   <li>DELETE: before=null (Scylla doesn't send preimage for PARTITION_DELETE), after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
 * deletes, so the "before" field will be null for DELETE operations.
 *
 * <p>This mixed mode is useful when consumers want to know what changed (minimal before) but need
 * complete current state (full after) for downstream processing.
 */
public class CdcIncludeBeforeOnlyUpdatedAfterFullIT
    extends CdcIncludeBeforeAfterBase<String, String> {

  private static final String BEFORE_MODE = "only-updated";
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
    pk = extractIdFromKeyField(value);
    if (pk != -1) {
      return pk;
    }
    return extractPkFromNameField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  /**
   * INSERT: before=null, after=full image.
   *
   * <p>INSERT always uses full image for after.
   */
  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, insertNameValue(pk), insertValueValue(pk), expectedSource())
    };
  }

  /**
   * DELETE: before=null, after=null.
   *
   * <p>This table has only a partition key (no clustering key), so DELETE operations are
   * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
   * deletes, so "before" is null. The "key" field contains the PK values for identification.
   */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, deleteNameValue(pk), deleteValueValue(pk), expectedSource()),
      // DELETE record: before=null because Scylla doesn't send preimage for PARTITION_DELETE
      """
        {
          "before": null,
          "after": null,
          "key": {"id": %d},
          "op": "d",
          "source": %s
        }
        """
          .formatted(pk, expectedSource()),
      // Tombstone record (null value) for Kafka log compaction
      null
    };
  }

  /**
   * UPDATE (single column): before=only modified column + PK, after=full image.
   *
   * <p>Mixed mode: before contains only the modified "name" column (no "value"), but after contains
   * ALL columns (full postimage state).
   */
  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), updateBeforeValueValue(pk), expectedSource()),
      // UPDATE record:
      // - before: ONLY-UPDATED (just "name" + PK, no "value")
      // - after: FULL (all columns including unchanged "value")
      """
        {
          "before": {"id": %d, "name": "%s"},
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              updateBeforeNameValue(pk),
              pk,
              updateAfterNameValue(pk),
              updateBeforeValueValue(pk), // value unchanged
              expectedSource())
    };
  }

  /**
   * UPDATE (multiple columns): before=all modified columns + PK, after=full image.
   *
   * <p>When all non-key columns are modified, the before struct will contain all of them.
   */
  @Override
  String[] expectedUpdateMultiColumn(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), updateBeforeValueValue(pk), expectedSource()),
      // UPDATE record:
      // - before: ONLY-UPDATED (both modified columns + PK)
      // - after: FULL (all columns)
      """
        {
          "before": {"id": %d, "name": "%s", "value": %d},
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk,
              updateBeforeNameValue(pk),
              updateBeforeValueValue(pk),
              pk,
              updateAfterNameValue(pk),
              updateAfterValueValue(pk),
              expectedSource())
    };
  }
}
