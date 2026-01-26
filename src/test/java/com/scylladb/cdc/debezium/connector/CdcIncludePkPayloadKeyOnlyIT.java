package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.primary-key.placement configuration with kafka-key and
 * payload-key only.
 *
 * <p>Tests verify that primary key columns appear in the Kafka record key and payload "key" field,
 * but NOT in payload-after or payload-before.
 *
 * <p>Note: kafka-key is required for reliable Kafka record delivery (partitioning).
 */
public class CdcIncludePkPayloadKeyOnlyIT extends CdcIncludePkBase<String, String> {

  private static final String PK_PLACEMENT = "kafka-key,payload-key";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, PK_PLACEMENT);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // Extract from payload "key" field
    int pk = extractIdFromKeyField(value);
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
      // INSERT record: PK NOT in after, but key field IS present in payload
      """
        {
          "before": null,
          "after": {"name": "%s"},
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(insertNameValue(pk), pk, expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    // With payload-key, DELETE records can be matched via key field
    // Plus tombstone matched via kafka-key
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"name": "%s"},
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(deleteNameValue(pk), pk, expectedSource()),
      // DELETE record: key field present in payload allows matching
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
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(updateBeforeNameValue(pk), pk, expectedSource()),
      // UPDATE record: PK NOT in before or after, but key field IS present
      """
        {
          "before": {"name": "%s"},
          "after": {"name": "%s"},
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(updateBeforeNameValue(pk), updateAfterNameValue(pk), pk, expectedSource())
    };
  }
}
