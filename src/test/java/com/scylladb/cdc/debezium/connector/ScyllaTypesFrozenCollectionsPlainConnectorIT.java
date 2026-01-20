package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsertWithValues(TestInfo testInfo) {
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedInsertWithEmpty(TestInfo testInfo) {
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedInsertWithNull(TestInfo testInfo) {
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFromValueToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFromValueToEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFromValueToNull(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFromEmptyToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFromNullToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
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
