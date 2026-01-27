package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesNonFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  private int extractIdFromJson(String json) {
    if (json == null) {
      return -1;
    }
    int idIndex = json.indexOf("\"id\":");
    if (idIndex == -1) {
      return -1;
    }
    int start = idIndex + 5;
    while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
      start++;
    }
    int end = start;
    while (end < json.length()
        && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
      end++;
    }
    if (end > start) {
      return Integer.parseInt(json.substring(start, end));
    }
    return -1;
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithValues(int pk) {
    // For non-frozen collections, lists are stored as maps with timeuuid keys internally
    // We skip checking list_col due to dynamic timeuuid keys
    return new String[] {
      """
        {
          "id": %d,
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(int pk) {
    // Skip checking list_col due to dynamic timeuuid keys
    return new String[] {
      """
        {
          "id": %d,
          "set_col": ["x"],
          "map_col": [{"key": 10, "value": "ten"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListAddElement(int pk) {
    // List add produces a record with dynamic timeuuid keys, just check id
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetAddElement(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "set_col": ["z"]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapAddElement(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "map_col": [{"key": 20, "value": "twenty"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListRemoveElement(int pk) {
    // List remove produces a record with dynamic timeuuid keys, just check id
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetRemoveElement(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "set_col": ["y"]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapRemoveElement(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "map_col": [{"key": 10, "value": null}]
        }
        """
          .formatted(pk)
    };
  }
}
