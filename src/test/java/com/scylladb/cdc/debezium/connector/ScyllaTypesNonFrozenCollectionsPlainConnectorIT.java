package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesNonFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
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
    // For non-frozen collections, lists are serialized as arrays of values (timeuuid keys are
    // discarded)
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "list_col": [10, 20, 30],
            "set_col": ["x", "y", "z"],
            "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]
          },
          "op": "c",
          "source": {
            "connector": "scylla",
            "name": "%s",
            "snapshot": "false",
            "db": "%s",
            "keyspace_name": "%s",
            "table_name": "%s"
          }
        }
        """
          .formatted(
              pk,
              getSuiteConnectorName(),
              getSuiteKeyspaceName(),
              getSuiteKeyspaceName(),
              getSuiteTableName())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "list_col": null,
            "set_col": null,
            "map_col": null
          },
          "op": "c",
          "source": {
            "connector": "scylla",
            "name": "%s",
            "snapshot": "false",
            "db": "%s",
            "keyspace_name": "%s",
            "table_name": "%s"
          }
        }
        """
          .formatted(
              pk,
              getSuiteConnectorName(),
              getSuiteKeyspaceName(),
              getSuiteKeyspaceName(),
              getSuiteTableName())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "list_col": [10],
            "set_col": ["x"],
            "map_col": [{"key": 10, "value": "ten"}]
          },
          "op": "c",
          "source": {
            "connector": "scylla",
            "name": "%s",
            "snapshot": "false",
            "db": "%s",
            "keyspace_name": "%s",
            "table_name": "%s"
          }
        }
        """
          .formatted(
              pk,
              getSuiteConnectorName(),
              getSuiteKeyspaceName(),
              getSuiteKeyspaceName(),
              getSuiteTableName()),
      // DELETE record
      expectedRecord(
          "d",
          """
            {
              "id": %d,
              "list_col": null,
              "set_col": null,
              "map_col": null
            }
            """
              .formatted(pk),
          "null"),
      // Tombstone
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListAddElement(int pk) {
    // List add produces a record with the added element value (timeuuid keys are discarded)
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "list_col": [30]
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetAddElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "set_col": ["z"]
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapAddElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "map_col": [{"key": 20, "value": "twenty"}]
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListRemoveElement(int pk) {
    // List remove produces a record with a null value representing the deleted element
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "list_col": [null]
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetRemoveElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "set_col": ["y"]
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapRemoveElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "map_col": [{"key": 10, "value": null}]
            }
            """
              .formatted(pk))
    };
  }
}
