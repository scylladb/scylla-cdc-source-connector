package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesNonFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  void waitAndAssert(KafkaConsumer<String, String> consumer, String[] expected) {
    waitAndAssertKafkaMessages(consumer, expected);
  }

  @Override
  String[] expectedInsertWithValues() {
    return new String[] {
      """
        {
          "id": 1,
          "list_col": {"mode": "OVERWRITE", "elements": [{"value": 10}, {"value": 20}, {"value": 30}]},
          "set_col": {"mode": "OVERWRITE", "elements": [{"element": "x", "added": true}, {"element": "y", "added": true}, {"element": "z", "added": true}]},
          "map_col": {"mode": "OVERWRITE", "elements": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]}
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithNull() {
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
  String[] expectedDelete() {
    return new String[] {
      """
        {
        }
        """
    };
  }

  @Override
  String[] expectedUpdateListAddElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "list_col": {"mode": "MODIFY", "elements": [{"value": 30}]}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateSetAddElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "set_col": {"mode": "MODIFY", "elements": [{"element": "z", "added": true}]}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateMapAddElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "map_col": {"mode": "MODIFY", "elements": [{"key": 20, "value": "twenty"}]}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateListRemoveElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "list_col": {"mode": "MODIFY", "elements": [{"value": null}]}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateSetRemoveElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "set_col": {"mode": "MODIFY", "elements": [{"element": "y", "added": false}]}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateMapRemoveElement() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "map_col": {"mode": "MODIFY", "elements": [{"key": 10, "value": null}]}
        }
        """
    };
  }
}
