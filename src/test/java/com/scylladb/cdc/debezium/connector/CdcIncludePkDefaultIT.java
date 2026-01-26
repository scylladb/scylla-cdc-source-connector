package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.primary-key.placement with the DEFAULT configuration.
 *
 * <p>Tests verify that primary key columns appear in the default locations: kafka-key,
 * payload-after, and payload-before. The payload "key" field should NOT be present.
 *
 * <p>Note: DELETE records have null before/after and no payload-key field, so they cannot be
 * matched. Only INSERT + tombstone can be verified for DELETE operations.
 */
public class CdcIncludePkDefaultIT extends CdcIncludePkBase<String, String> {

  private static final String PK_PLACEMENT = "kafka-key,payload-after,payload-before";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, PK_PLACEMENT);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // First try to extract from "after" field
    int pk = extractIdFromAfter(value);
    if (pk != -1) {
      return pk;
    }
    // Fallback to name field
    return extractPkFromNameField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      // INSERT record: PK in after, NO key field in payload (default config)
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, insertNameValue(pk), expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    // Without payload-key, DELETE records cannot be matched (null before/after)
    // Only INSERT + tombstone (via kafka-key) can be verified
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, deleteNameValue(pk), expectedSource()),
      // Tombstone record (matched via kafka-key)
      null
    };
  }

  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), expectedSource()),
      // UPDATE record: PK in before and after, NO key field
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
}
