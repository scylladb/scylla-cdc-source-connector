package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractPkFromNameField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.primary-key.placement configuration with ALL locations enabled
 * (except payload-diff).
 *
 * <p>Tests verify that primary key columns appear in all configured locations: kafka-key,
 * payload-after, payload-before, payload-key, and kafka-headers.
 */
public class CdcIncludePkAllLocationsIT extends CdcIncludePkBase<String, String> {

  private static final String PK_PLACEMENT =
      "kafka-key,payload-after,payload-before,payload-key,kafka-headers";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, PK_PLACEMENT);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // First try to extract from payload "key" field
    int pk = extractIdFromKeyField(value);
    if (pk != -1) {
      return pk;
    }
    // Try from after field
    pk = extractIdFromAfter(value);
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
      // INSERT record: PK in after, key field present
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s"},
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, insertNameValue(pk), pk, expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    // With payload-key enabled, DELETE record can be matched via key field
    // Plus tombstone matched via kafka-key
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {"id": %d, "name": "%s"},
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, deleteNameValue(pk), pk, expectedSource()),
      // DELETE record: key field present allows matching
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
          "after": {"id": %d, "name": "%s"},
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, updateBeforeNameValue(pk), pk, expectedSource()),
      // UPDATE record: PK in both before and after, key field present
      """
        {
          "before": {"id": %d, "name": "%s"},
          "after": {"id": %d, "name": "%s"},
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              pk, updateBeforeNameValue(pk), pk, updateAfterNameValue(pk), pk, expectedSource())
    };
  }
}
