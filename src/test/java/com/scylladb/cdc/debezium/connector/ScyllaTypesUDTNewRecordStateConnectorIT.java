package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesUDTNewRecordStateConnectorIT extends ScyllaTypesUDTBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
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
          "id": %d,
          "frozen_udt": {"a": 42, "b": "foo"},
          "nf_udt": {"mode": "OVERWRITE", "elements": {"a": {"value": 7}, "b": {"value": "bar"}}},
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}},
          "frozen_udt_with_map": {"m": {"key1": 100, "key2": 200}},
          "nf_udt_with_map": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithFrozenUdt(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "frozen_udt": {"a": 42, "b": "foo"},
          "nf_udt": null,
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}},
          "frozen_udt_with_map": {"m": {"key1": 100, "key2": 200}},
          "nf_udt_with_map": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdt(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "frozen_udt": null,
          "nf_udt": {"mode": "OVERWRITE", "elements": {"a": {"value": 7}, "b": {"value": "bar"}}},
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}},
          "frozen_udt_with_map": {"m": {"key1": 100, "key2": 200}},
          "nf_udt_with_map": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}
        }
        """
          .formatted(pk)
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
      """
        {
          "id": %d,
          "frozen_udt": {"a": 42, "b": "foo"},
          "nf_udt": {"mode": "OVERWRITE", "elements": {"a": {"value": 7}, "b": {"value": "bar"}}},
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"mode": "OVERWRITE", "elements": {"inner": {"value": {"x": 10, "y": "hello"}}, "z": {"value": 20}}},
          "frozen_udt_with_map": {"m": {"key1": 100, "key2": 200}},
          "nf_udt_with_map": {"mode": "OVERWRITE", "elements": {"m": {"value": {"key1": 100, "key2": 200}}}},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"mode": "OVERWRITE", "elements": {"l": {"value": [1, 2, 3]}}},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"mode": "OVERWRITE", "elements": {"s": {"value": ["a", "b", "c"]}}}
        }
        """
          .formatted(pk),
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_udt": {"a": 99, "b": "updated"},
          "nf_udt": {"mode": "MODIFY", "elements": {"a": {"value": 77}, "b": null}},
          "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
          "nf_nested_udt": {"mode": "MODIFY", "elements": {"inner": null, "z": {"value": 21}}},
          "frozen_udt_with_map": {"m": {"key1": 101, "key3": 300}},
          "nf_udt_with_map": {"mode": "MODIFY", "elements": {"m": {"value": {"key1": 101, "key3": 300}}}},
          "frozen_udt_with_list": {"l": [4, 5, 6]},
          "nf_udt_with_list": {"mode": "MODIFY", "elements": {"l": {"value": [4, 5, 6]}}},
          "frozen_udt_with_set": {"s": ["d", "e"]},
          "nf_udt_with_set": {"mode": "MODIFY", "elements": {"s": {"value": ["d", "e"]}}}
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_udt": null,
          "nf_udt": null,
          "frozen_nested_udt": null,
          "nf_nested_udt": null,
          "frozen_udt_with_map": null,
          "nf_udt_with_map": {"mode": "MODIFY", "elements": {"m": {"value": {}}}},
          "frozen_udt_with_list": null,
          "nf_udt_with_list": {"mode": "MODIFY", "elements": {"l": {"value": []}}},
          "frozen_udt_with_set": null,
          "nf_udt_with_set": {"mode": "MODIFY", "elements": {"s": {"value": []}}}
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_udt": {"a": 99, "b": "updated"},
          "nf_udt": {"mode": "MODIFY", "elements": {"a": {"value": 100}, "b": null}},
          "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
          "nf_nested_udt": {"mode": "MODIFY", "elements": {"inner": null, "z": {"value": 21}}},
          "frozen_udt_with_map": {"m": {"key1": 101, "key3": 300}},
          "nf_udt_with_map": {"mode": "MODIFY", "elements": {"m": {"value": {"key1": 101, "key3": 300}}}},
          "frozen_udt_with_list": {"l": [4, 5, 6]},
          "nf_udt_with_list": {"mode": "MODIFY", "elements": {"l": {"value": [4, 5, 6]}}},
          "frozen_udt_with_set": {"s": ["d", "e"]},
          "nf_udt_with_set": {"mode": "MODIFY", "elements": {"s": {"value": ["d", "e"]}}}
        }
        """
          .formatted(pk)
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
