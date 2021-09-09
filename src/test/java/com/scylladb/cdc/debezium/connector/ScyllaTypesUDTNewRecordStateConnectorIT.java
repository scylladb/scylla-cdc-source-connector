package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesUDTNewRecordStateConnectorIT extends ScyllaTypesUDTBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsert(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
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
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
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
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
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
        }
        """
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
      """
        {
          "id": 1,
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
        }
        """,
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_udt": {"a": 99, "b": "updated"},
          "nf_udt": {"a": 77},
          "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
          "nf_nested_udt": {"z": 21},
          "frozen_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
          "nf_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
          "frozen_udt_with_list": {"l": [4, 5, 6]},
          "nf_udt_with_list": {"l": [4, 5, 6]},
          "frozen_udt_with_set": {"s": ["d", "e"]},
          "nf_udt_with_set": {"s": ["d", "e"]}
        }
        """
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
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
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_udt": {"a": 99, "b": "updated"},
          "nf_udt": {"a": 100},
          "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
          "nf_nested_udt": {"z": 21},
          "frozen_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
          "nf_udt_with_map": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]},
          "frozen_udt_with_list": {"l": [4, 5, 6]},
          "nf_udt_with_list": {"l": [4, 5, 6]},
          "frozen_udt_with_set": {"s": ["d", "e"]},
          "nf_udt_with_set": {"s": ["d", "e"]}
        }
        """
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
