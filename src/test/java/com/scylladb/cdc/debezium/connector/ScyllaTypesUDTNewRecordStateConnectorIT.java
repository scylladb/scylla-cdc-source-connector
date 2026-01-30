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
          "nf_udt": {"a": 7, "b": "bar"},
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"l": [1, 2, 3]},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"s": ["a", "b", "c"]}
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      """
        {
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
          "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"l": [1, 2, 3]},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
          "nf_udt": {"a": 7, "b": "bar"},
          "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
          "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
          "frozen_udt_with_list": {"l": [1, 2, 3]},
          "nf_udt_with_list": {"l": [1, 2, 3]},
          "frozen_udt_with_set": {"s": ["a", "b", "c"]},
          "nf_udt_with_set": {"s": ["a", "b", "c"]}
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
      // INSERT record
      """
        {
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
        }
        """
          .formatted(pk),
      // DELETE record - after is null, so the record is null
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
    // Full before/after states with postimage enabled
    // Empty UDTs (all fields null) are represented as null for consistency
    return new String[] {
      // INSERT record
      """
        {
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
        }
        """
          .formatted(pk),
      // UPDATE record with full after state (postimage)
      // nf_udt.a updated from 42 to 77, b preserved as "foo"
      // nf_nested_udt preserves inner from original, only z changes
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(int pk) {
    // Full before/after states with postimage enabled
    // Empty UDTs (all fields null) are represented as null for consistency
    return new String[] {
      // INSERT record
      """
        {
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
        }
        """
          .formatted(pk),
      // UPDATE record with full after state (postimage)
      // Empty UDTs (all fields null) are now represented as null
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(int pk) {
    // Full before/after states with postimage enabled
    return new String[] {
      // INSERT record
      """
        {
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
        }
        """
          .formatted(pk),
      // UPDATE record with full after state (postimage)
      // nf_udt preserves b='bar' from original, a changes to 100
      // nf_nested_udt preserves inner from original, z changes to 21
      """
        {
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

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToEmpty(int pk) {
    return new String[] {
      // Preimage: initial state with all UDT values
      """
        {
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
        }
        """
          .formatted(pk),
      // Postimage: empty UDTs (all fields null) become null
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromNullToValue(int pk) {
    return new String[] {
      // Preimage: initial state with null UDTs
      """
        {
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
        }
        """
          .formatted(pk),
      // Postimage: populated UDTs
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromEmptyToValue(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      // Preimage: empty UDTs (all fields null) become null
      """
        {
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
        }
        """
          .formatted(pk),
      // Postimage: populated UDTs
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      // Preimage: initial state with all UDT values
      """
        {
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
        }
        """
          .formatted(pk),
      // Postimage: non-frozen UDTs replaced with new values
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      // Preimage: initial state with all UDT values
      """
        {
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
        }
        """
          .formatted(pk),
      // Postimage: non-frozen UDTs set to null
      """
        {
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
        }
        """
          .formatted(pk)
    };
  }
}
