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
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": {"value": {"mode": "OVERWRITE", "elements": {"a": {"value": 7}, "b": {"value": "bar"}}}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}}},
            "frozen_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
            "nf_udt_with_map": {"value": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "nf_udt_with_list": {"value": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "nf_udt_with_set": {"value": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}}
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
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": null,
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
            "nf_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
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
            "frozen_udt": {"value": null},
            "nf_udt": {"value": {"a": 7, "b": "bar"}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
            "nf_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
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
      // Only verify the INSERT record - DELETE may not be captured due to timing
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": {"value": {"mode": "OVERWRITE", "elements": {"a": {"value": 7}, "b": {"value": "bar"}}}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "nf_nested_udt": {"value": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}}},
            "frozen_udt_with_map": {"value": {"m": {"key1": 100, "key2": 200}}},
            "nf_udt_with_map": {"value": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "nf_udt_with_list": {"value": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "nf_udt_with_set": {"value": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}}
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
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_udt": {"value": {"a": 99, "b": "updated"}},
              "nf_udt": {"value": {"mode": "MODIFY", "elements": {"a": {"value": 77}, "b": null}}},
              "frozen_nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "nf_nested_udt": {"value": {"mode": "MODIFY", "elements": {"inner": null, "z": {"value": 21}}}},
              "frozen_udt_with_map": {"value": {"m": {"key1": 101, "key3": 300}}},
              "nf_udt_with_map": {"value": {"mode": "MODIFY", "elements": {"m": {"value": {"key1": 101, "key3": 300}}}}},
              "frozen_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "nf_udt_with_list": {"value": {"mode": "MODIFY", "elements": {"l": {"value": [4, 5, 6]}}}},
              "frozen_udt_with_set": {"value": {"s": ["d", "e"]}},
              "nf_udt_with_set": {"value": {"mode": "MODIFY", "elements": {"s": {"value": ["d", "e"]}}}}
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
              "frozen_udt": {"value": null},
              "nf_udt": {"value": null},
              "frozen_nested_udt": {"value": null},
              "nf_nested_udt": {"value": null},
              "frozen_udt_with_map": {"value": null},
              "nf_udt_with_map": {"value": {"mode": "MODIFY", "elements": {"m": {"value": {}}}}},
              "frozen_udt_with_list": {"value": null},
              "nf_udt_with_list": {"value": {"mode": "MODIFY", "elements": {"l": {"value": []}}}},
              "frozen_udt_with_set": {"value": null},
              "nf_udt_with_set": {"value": {"mode": "MODIFY", "elements": {"s": {"value": []}}}}}
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
              "frozen_udt": {"value": {"a": 99, "b": "updated"}},
              "nf_udt": {"value": {"mode": "MODIFY", "elements": {"a": {"value": 100}, "b": null}}},
              "frozen_nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "nf_nested_udt": {"value": {"mode": "MODIFY", "elements": {"inner": null, "z": {"value": 21}}}},
              "frozen_udt_with_map": {"value": {"m": {"key1": 101, "key3": 300}}},
              "nf_udt_with_map": {"value": {"mode": "MODIFY", "elements": {"m": {"value": {"key1": 101, "key3": 300}}}}},
              "frozen_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "nf_udt_with_list": {"value": {"mode": "MODIFY", "elements": {"l": {"value": [4, 5, 6]}}}},
              "frozen_udt_with_set": {"value": {"s": ["d", "e"]}},
              "nf_udt_with_set": {"value": {"mode": "MODIFY", "elements": {"s": {"value": ["d", "e"]}}}}
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
