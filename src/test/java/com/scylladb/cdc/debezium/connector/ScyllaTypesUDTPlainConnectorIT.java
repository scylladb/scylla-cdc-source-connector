package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesUDTPlainConnectorIT extends ScyllaTypesUDTBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsert(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": {"value": {"a": 7, "b": "bar"}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "nf_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "nf_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "nf_udt_with_set": {"value": {"s": ["a", "b", "c"]}}
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
  String[] expectedInsertWithFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": null,
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "nf_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "nf_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "nf_udt_with_set": {"value": {"s": ["a", "b", "c"]}}
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
  String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": null},
            "nf_udt": {"value": {"a": 7, "b": "bar"}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "nf_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "nf_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "nf_udt_with_set": {"value": {"s": ["a", "b", "c"]}}
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
  String[] expectedInsertWithNonFrozenNestedUdt(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithMap(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithList(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithSet(TestInfo testInfo) {
    return new String[] {};
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
              "frozen_udt": null,
              "nf_udt": null,
              "frozen_nested_udt": null,
              "nf_nested_udt": null,
              "frozen_udt_with_map": null,
              "nf_udt_with_map": null,
              "frozen_udt_with_list": null,
              "nf_udt_with_list": null,
              "frozen_udt_with_set": null,
              "nf_udt_with_set": null
            }
            """,
          "null"),
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_udt": {"value": {"a": 99, "b": "updated"}},
              "nf_udt": {"value": {"a": 77}},
              "frozen_nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "nf_nested_udt": {"value": {"z": 21}},
              "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "nf_udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "frozen_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "nf_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "frozen_udt_with_set": {"value": {"s": ["d", "e"]}},
              "nf_udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_udt": {"value": null},
              "nf_udt": {"value": null},
              "frozen_nested_udt": {"value": null},
              "nf_nested_udt": {"value": null},
              "frozen_udt_with_map": {"value": null},
              "nf_udt_with_map": {"value": {"m": []}},
              "frozen_udt_with_list": {"value": null},
              "nf_udt_with_list": {"value": {"l": []}},
              "frozen_udt_with_set": {"value": null},
              "nf_udt_with_set": {"value": {"s": []}}}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_udt": {"value": {"a": 99, "b": "updated"}},
              "nf_udt": {"value": {"a": 100}},
              "frozen_nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "nf_nested_udt": {"value": {"z": 21}},
              "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "nf_udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "frozen_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "nf_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "frozen_udt_with_set": {"value": {"s": ["d", "e"]}},
              "nf_udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenNestedUdtField(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithMapField(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithListField(TestInfo testInfo) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithSetField(TestInfo testInfo) {
    return new String[] {};
  }
}
