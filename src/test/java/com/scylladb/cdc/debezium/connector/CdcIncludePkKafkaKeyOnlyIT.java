package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.primary-key.placement configuration with ONLY kafka-key
 * enabled.
 *
 * <p>Tests verify that primary key columns appear ONLY in the Kafka record key, and NOT in
 * payload-after, payload-before, payload-key, or kafka-headers.
 *
 * <p>Note: DELETE records have null before/after and no payload-key field, so they cannot be
 * matched. Only INSERT + tombstone can be verified for DELETE operations.
 */
public class CdcIncludePkKafkaKeyOnlyIT extends CdcIncludePkBase<String, String> {

  private static final String PK_PLACEMENT = "kafka-key";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, PK_PLACEMENT);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // With kafka-key only, PK is not in payload, so extract from name field
    return extractPkFromNameField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Override
  String[] expectedInsert(int pk) {
    return new String[] {
      // INSERT record: PK NOT in after, NO key field in payload
      """
        {
          "before": null,
          "after": {"name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(insertNameValue(pk), expectedSource())
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
          "after": {"name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(deleteNameValue(pk), expectedSource()),
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
          "after": {"name": "%s"},
          "op": "c",
          "source": %s
        }
        """
          .formatted(updateBeforeNameValue(pk), expectedSource()),
      // UPDATE record: PK NOT in before or after, NO key field
      """
        {
          "before": {"name": "%s"},
          "after": {"name": "%s"},
          "op": "u",
          "source": %s
        }
        """
          .formatted(updateBeforeNameValue(pk), updateAfterNameValue(pk), expectedSource())
    };
  }
}
