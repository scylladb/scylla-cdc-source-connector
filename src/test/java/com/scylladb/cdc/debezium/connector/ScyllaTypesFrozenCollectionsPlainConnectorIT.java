package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
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
          "before": null,
          "after": {
            "id": 1,
            "frozen_list_col": {"value": [1, 2, 3]},
            "frozen_set_col": {"value": ["a", "b", "c"]},
            "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
            "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
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
          .formatted(connectorName(), KEYSPACE, KEYSPACE, tableNameOnly())
    };
  }

  @Override
  String[] expectedInsertWithEmpty() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_list_col": {"value": []},
            "frozen_set_col": {"value": []},
            "frozen_map_col": {"value": []},
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
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
          .formatted(connectorName(), KEYSPACE, KEYSPACE, tableNameOnly())
    };
  }

  @Override
  String[] expectedInsertWithNull() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_list_col": {"value": null},
            "frozen_set_col": {"value": null},
            "frozen_map_col": {"value": null},
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
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
          .formatted(connectorName(), KEYSPACE, KEYSPACE, tableNameOnly())
    };
  }

  @Override
  String[] expectedDelete() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "d",
          """
            {
              "id": 1
            }
            """,
          "null"),
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [4, 5, 6]},
              "frozen_set_col": {"value": ["x", "y", "z"]},
              "frozen_map_col": {"value": [[3, "three"], [4, "four"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 99, "tuple_member_1": "bar"}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": []},
              "frozen_set_col": {"value": []},
              "frozen_map_col": {"value": []},
              "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateFromValueToNull() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": null},
              "frozen_set_col": {"value": null},
              "frozen_map_col": {"value": null},
              "frozen_tuple_col": {"value": null}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateFromNullToValue() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
            }
            """)
    };
  }
}
