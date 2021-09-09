package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesNonFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithValues(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "list_col": [{"value": 10}, {"value": 20}, {"value": 30}],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "list_col": null,
          "set_col": null,
          "map_col": null
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "list_col": [{"value": 10}],
          "set_col": ["x"],
          "map_col": [{"key": 10, "value": "ten"}]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListAddElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "list_col": [{"value": 30}]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetAddElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "set_col": ["z"]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapAddElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "map_col": [{"key": 20, "value": "twenty"}]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListRemoveElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "list_col": [{"value": null}]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetRemoveElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "set_col": ["y"]
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapRemoveElement(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "map_col": [{"key": 10, "value": null}]
        }
        """
    };
  }
}
