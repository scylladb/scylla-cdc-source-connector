package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
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
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"}
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithEmpty() {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_list_col": [],
          "frozen_set_col": [],
          "frozen_map_col": [],
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null}
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
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null,
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null}
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
  String[] expectedUpdateFromValueToValue() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_list_col": [4, 5, 6],
          "frozen_set_col": ["x", "y", "z"],
          "frozen_map_col": [[3, "three"], [4, "four"]],
          "frozen_tuple_col": {"tuple_member_0": 99, "tuple_member_1": "bar"}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_list_col": [],
          "frozen_set_col": [],
          "frozen_map_col": [],
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFromValueToNull() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null,
          "frozen_tuple_col": null
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFromNullToValue() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"}
        }
        """
    };
  }
}
