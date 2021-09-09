package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesNonFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithValues(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "list_col": {"value": [{"value": 10}, {"value": 20}, {"value": 30}]},
            "set_col": {"value": ["x", "y", "z"]},
            "map_col": {"value": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]}
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
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "list_col": {"value": null},
            "set_col": {"value": null},
            "map_col": {"value": null}
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
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "d",
          """
            {
              "id": 1,
              "list_col": null,
              "set_col": null,
              "map_col": null
            }
            """,
          "null"),
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": 30}]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "set_col": {"value": ["z"]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "map_col": {"value": [{"key": 20, "value": "twenty"}]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": null}]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "set_col": {"value": ["y"]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "map_col": {"value": [{"key": 10, "value": null}]}
            }
            """)
    };
  }
}
