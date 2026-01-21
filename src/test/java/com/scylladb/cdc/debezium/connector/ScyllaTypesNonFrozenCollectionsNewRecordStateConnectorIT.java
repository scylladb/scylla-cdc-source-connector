package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesNonFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsertWithValues(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]
        }
        """
    };
  }

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

  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """
    };
  }

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
          "list_col": [30]
        }
        """
    };
  }

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
          "list_col": null
        }
        """
    };
  }

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
          "set_col": null
        }
        """
    };
  }

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
          "map_col": null
        }
        """
    };
  }
}
