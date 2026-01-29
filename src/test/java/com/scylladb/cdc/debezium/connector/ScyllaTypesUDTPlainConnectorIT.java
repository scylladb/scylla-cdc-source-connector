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

  /** Track the current test PK for use in expectedKey(). */
  private int currentPk;

  /** {@inheritDoc} */
  @Override
  protected String expectedKey() {
    return "{\"id\": " + currentPk + "}";
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsert(int pk) {
    currentPk = pk;
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
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(int pk) {
    currentPk = pk;
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithEmpty(int pk) {
    currentPk = pk;
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithFrozenUdt(int pk) {
    currentPk = pk;
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
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdt(int pk) {
    currentPk = pk;
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
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource())
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
    currentPk = pk;
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
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // DELETE record - for partition deletes (tables without clustering key),
      // preimage is not sent in Scylla versions < 2026.1.0
      """
        {
          "before": null,
          "after": null,
          "key": {"id": %d},
          "op": "d",
          "source": %s
        }
        """
          .formatted(pk, expectedSource()),
      // Tombstone
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with all UDT values
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 42, "b": "foo"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE with frozen UDT changes
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 42, "b": "foo"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 99, "b": "updated"},
            "nf_udt": {"a": 77, "b": "foo"},
            "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
            "frozen_udt_with_list": {"l": [4, 5, 6]},
            "nf_udt_with_list": {"l": [4, 5, 6]},
            "frozen_udt_with_set": {"s": ["d", "e"]},
            "nf_udt_with_set": {"s": ["d", "e"]}
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with initial values
      // Note: non-frozen UDTs that are null are represented as null
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": null,
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE setting frozen UDTs to null
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": null,
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with initial values (nf_udt has initial value)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": null,
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE with non-frozen UDT field change
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": null,
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 99, "b": "updated"},
            "nf_udt": {"a": 100, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
            "frozen_udt_with_list": {"l": [4, 5, 6]},
            "nf_udt_with_list": {"l": [4, 5, 6]},
            "frozen_udt_with_set": {"s": ["d", "e"]},
            "nf_udt_with_set": {"s": ["d", "e"]}
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
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

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToEmpty(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with all UDT values
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE setting UDTs to empty (all fields null)
      // Empty UDTs are represented as null in Scylla
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromNullToValue(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with all UDT values null
      """
        {
          "before": null,
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE setting UDTs to populated values
      """
        {
          "before": {
            "id": %d,
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
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromEmptyToValue(int pk) {
    currentPk = pk;
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      // First record: INSERT with empty UDTs (all fields null -> null)
      """
        {
          "before": null,
          "after": {
            "id": %d,
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
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE setting UDTs to populated values
      """
        {
          "before": {
            "id": %d,
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
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToValue(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with all UDT values
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE replacing non-frozen UDTs entirely
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 99, "b": "updated"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}, {"key": "81d4a033-4632-11f0-9484-409dd8f36eba", "value": "value4"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [4, 5, 6]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["d", "e", "f"]}
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToNull(int pk) {
    currentPk = pk;
    return new String[] {
      // First record: INSERT with all UDT values
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(pk, pk, expectedSource()),
      // Second record: UPDATE setting non-frozen UDTs to null
      """
        {
          "before": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": {"a": 7, "b": "bar"},
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": {"l": [1, 2, 3]},
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": {"s": ["a", "b", "c"]}
          },
          "after": {
            "id": %d,
            "frozen_udt": {"a": 42, "b": "foo"},
            "nf_udt": null,
            "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
            "nf_nested_udt": null,
            "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
            "nf_udt_with_map": null,
            "frozen_udt_with_list": {"l": [1, 2, 3]},
            "nf_udt_with_list": null,
            "frozen_udt_with_set": {"s": ["a", "b", "c"]},
            "nf_udt_with_set": null
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(pk, pk, pk, expectedSource())
    };
  }
}
