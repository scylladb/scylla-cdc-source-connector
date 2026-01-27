package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesUDTPlainConnectorIT extends ScyllaTypesUDTBase<String, String> {
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
  String[] expectedInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "nf_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
  String[] expectedInsertWithFrozenUdt(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": null,
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "nf_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
  String[] expectedInsertWithNonFrozenUdt(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": null,
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "nf_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
  String[] expectedInsertWithNonFrozenNestedUdt(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithMap(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithList(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdtWithSet(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // Verify the INSERT record
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "nf_udt_with_map": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
      // DELETE may produce additional records depending on CDC configuration
      expectedRecord(
          "d",
          """
            {
              "id": %d
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
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_udt": {"a": 99, "b": "updated"},
              "nf_udt": {"a": 77, "b": null},
              "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
              "nf_nested_udt": {"inner": null, "z": 21},
              "frozen_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
              "nf_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
              "frozen_udt_with_list": {"l": [4, 5, 6]},
              "nf_udt_with_list": {"l": [4, 5, 6]},
              "frozen_udt_with_set": {"s": ["d", "e"]},
              "nf_udt_with_set": {"s": ["d", "e"]}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_udt": null,
              "nf_udt": null,
              "frozen_nested_udt": null,
              "nf_nested_udt": null,
              "frozen_udt_with_map": null,
              "nf_udt_with_map": {"m": []},
              "frozen_udt_with_list": null,
              "nf_udt_with_list": {"l": []},
              "frozen_udt_with_set": null,
              "nf_udt_with_set": {"s": []}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_udt": {"a": 99, "b": "updated"},
              "nf_udt": {"a": 100, "b": null},
              "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
              "nf_nested_udt": {"inner": null, "z": 21},
              "frozen_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
              "nf_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
              "frozen_udt_with_list": {"l": [4, 5, 6]},
              "nf_udt_with_list": {"l": [4, 5, 6]},
              "frozen_udt_with_set": {"s": ["d", "e"]},
              "nf_udt_with_set": {"s": ["d", "e"]}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenNestedUdtField(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithMapField(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithListField(int pk) {
    return new String[] {};
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtWithSetField(int pk) {
    return new String[] {};
  }
}
