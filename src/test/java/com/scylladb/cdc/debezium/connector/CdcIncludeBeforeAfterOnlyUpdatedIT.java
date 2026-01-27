package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromBefore;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;

/**
 * Integration tests for cdc.include.before=only-updated and cdc.include.after=only-updated.
 *
 * <p>DISABLED: The only-updated mode is not yet implemented. These tests are reserved for future
 * implementation.
 *
 * <p>Tests verify that for UPDATE operations, only the modified columns are included in the before
 * and after structs (plus primary key based on pk placement configuration).
 *
 * <p>Expected behavior by operation type:
 *
 * <ul>
 *   <li>INSERT: before=null, after=full image (all columns with values + PK)
 *   <li>UPDATE: before=only modified columns (old values) + PK, after=only modified columns (new
 *       values) + PK
 *   <li>DELETE: before=null (Scylla doesn't send preimage for PARTITION_DELETE), after=null
 * </ul>
 *
 * <p>Note: This table has only a partition key (no clustering key), so DELETE operations are
 * represented as PARTITION_DELETE by Scylla CDC. Scylla doesn't send preimage for partition
 * deletes, so the "before" field will be null for DELETE operations.
 *
 * <p>The "only-updated" mode is particularly useful for reducing message size and for consumers
 * that only care about what changed rather than the full row state.
 */
@Disabled("only-updated mode is not yet implemented")
public class CdcIncludeBeforeAfterOnlyUpdatedIT extends CdcIncludeBeforeAfterBase<String, String> {

  private static final String BEFORE_MODE = "only-updated";
  private static final String AFTER_MODE = "only-updated";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, BEFORE_MODE, AFTER_MODE);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // First try to extract from "after" field
    int pk = extractIdFromAfter(value);
    if (pk != -1) {
      return pk;
    }
    // Then try "before" field (for DELETE records)
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
    // Fallback to name field parsing
    return extractPkFromNameField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  /**
   * INSERT: before=null, after=full image.
   *
   * <p>For INSERT operations, the "only-updated" mode has no effect because there is no previous
   * state. The after struct contains all columns with their values.
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
      // INSERT record (CREATE operation)
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
   * UPDATE (single column): before and after contain only modified column + PK.
   *
   * <p>When only the "name" column is updated, the "value" column should NOT appear in either the
   * before or after structs. Only the modified "name" column (plus PK) should be present.
   */
  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record (CREATE operation)
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), updateBeforeValueValue(pk), expectedSource()),
      // UPDATE record - only modified column (name) + PK in before/after
      // The "value" column should NOT be present since it was not modified
      """
        {
          "before": {"id": %d, "name": "%s"},
          "after": {"id": %d, "name": "%s"},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), pk, updateAfterNameValue(pk), expectedSource())
    };
  }

  /**
   * UPDATE (multiple columns): before and after contain all modified columns + PK.
   *
   * <p>When both "name" and "value" columns are updated, both should appear in the before and after
   * structs (along with PK).
   */
  @Override
  String[] expectedUpdateMultiColumn(int pk) {
    return new String[] {
      // INSERT record (CREATE operation)
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s", "value": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), updateBeforeValueValue(pk), expectedSource()),
      // UPDATE record - both modified columns (name, value) + PK in before/after
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
