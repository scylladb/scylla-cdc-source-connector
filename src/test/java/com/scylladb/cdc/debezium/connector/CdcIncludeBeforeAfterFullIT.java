package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

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
   * <p>Same as only-updated mode for INSERT operations.
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
   * UPDATE (single column): before and after contain ALL columns.
   *
   * <p>Unlike only-updated mode, even when only "name" is modified, the before struct should
   * contain both "name" and "value" columns (reconstructed from preimage + postimage), and the
   * after struct should contain all columns from postimage.
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
      // UPDATE record - ALL columns in before/after (including unchanged "value")
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
              updateBeforeValueValue(pk), // value unchanged
              expectedSource())
    };
  }

  /**
   * UPDATE (multiple columns): before and after contain ALL columns.
   *
   * <p>When both columns are modified, all columns should appear in both structs.
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
      // UPDATE record - ALL columns in before/after
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
