package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;

/**
 * Integration tests for mixed mode: cdc.include.before=full and cdc.include.after=only-updated.
 *
 * <p>DISABLED: The only-updated mode is not yet implemented. These tests are reserved for future
 * implementation.
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
 *   <li>DELETE: before=null (Scylla doesn't send preimage for PARTITION_DELETE), after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
 * deletes, so the "before" field will be null for DELETE operations.
 *
 * <p>This mixed mode is useful when consumers need complete state for before (e.g., for auditing)
 * but want minimal data for after (e.g., for efficient change propagation).
 */
@Disabled("only-updated mode is not yet implemented")
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
   * <p>INSERT always uses full image for after, regardless of the only-updated setting.
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
   * UPDATE (single column): before=full image, after=only modified column + PK.
   *
   * <p>Mixed mode: before contains ALL columns (full state), but after contains only the modified
   * "name" column (no "value" column since it wasn't modified).
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
      // - before: FULL (all columns including unchanged "value")
      // - after: ONLY-UPDATED (just "name" + PK, no "value")
      """
        {
          "before": {"id": %d, "name": "%s", "value": %d},
          "after": {"id": %d, "name": "%s"},
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
              expectedSource())
    };
  }

  /**
   * UPDATE (multiple columns): before=full image, after=all modified columns + PK.
   *
   * <p>When all non-key columns are modified, the after struct will contain all of them.
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
      // - before: FULL (all columns)
      // - after: ONLY-UPDATED (both modified columns + PK)
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
