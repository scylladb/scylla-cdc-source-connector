package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesUDTAvroConnectorIT
    extends ScyllaTypesUDTBase<GenericRecord, GenericRecord> {

  @BeforeAll
  @Override
  public void setupSuite(TestInfo testInfo) {
    Assumptions.assumeTrue(
        KAFKA_PROVIDER == KafkaProvider.CONFLUENT, "Avro tests require Confluent Kafka provider");
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "Avro tests require distributed mode, otherwise Avro converter is not available");
    super.setupSuite(testInfo);
  }

  @Override
  KafkaConsumer<GenericRecord, GenericRecord> buildConsumer(
      String connectorName, String tableName) {
    return buildAvroConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(GenericRecord value) {
    return extractIdFromKeyField(value);
  }

  @Override
  protected int extractPkFromKey(GenericRecord key) {
    return extractIdFromRecord(key);
  }

  private int extractIdFromKeyField(GenericRecord record) {
    if (record == null) {
      return -1;
    }
    // Try to get "key" field first (payload-key)
    if (record.getSchema().getField("key") != null) {
      Object key = record.get("key");
      if (key instanceof GenericRecord) {
        GenericRecord keyRecord = (GenericRecord) key;
        if (keyRecord.getSchema().getField("id") != null) {
          Object id = keyRecord.get("id");
          if (id instanceof Number) {
            return ((Number) id).intValue();
          }
        }
      }
    }
    // Fallback to after/before/direct for backwards compatibility
    return extractIdFromRecord(record);
  }

  private int extractIdFromRecord(GenericRecord record) {
    if (record == null) {
      return -1;
    }
    // Try to get "after" field first (standard Debezium envelope)
    if (record.getSchema().getField("after") != null) {
      Object after = record.get("after");
      if (after instanceof GenericRecord) {
        GenericRecord afterRecord = (GenericRecord) after;
        if (afterRecord.getSchema().getField("id") != null) {
          Object id = afterRecord.get("id");
          if (id instanceof Number) {
            return ((Number) id).intValue();
          }
        }
      }
    }
    // Try "before" field (for delete operations)
    if (record.getSchema().getField("before") != null) {
      Object before = record.get("before");
      if (before instanceof GenericRecord) {
        GenericRecord beforeRecord = (GenericRecord) before;
        if (beforeRecord.getSchema().getField("id") != null) {
          Object id = beforeRecord.get("id");
          if (id instanceof Number) {
            return ((Number) id).intValue();
          }
        }
      }
    }
    // Fallback to direct "id" field (for keys)
    if (record.getSchema().getField("id") != null) {
      Object id = record.get("id");
      if (id instanceof Number) {
        return ((Number) id).intValue();
      }
    }
    return -1;
  }

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

  @Override
  String[] expectedInsertWithNull(int pk) {
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

  @Override
  String[] expectedInsertWithEmpty(int pk) {
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

  @Override
  String[] expectedInsertWithNonFrozenNestedUdt(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedInsertWithNonFrozenUdtWithMap(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedInsertWithNonFrozenUdtWithList(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedInsertWithNonFrozenUdtWithSet(int pk) {
    return new String[] {};
  }

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

  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(int pk) {
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

  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      // First record: INSERT with initial values
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

  @Override
  String[] expectedUpdateNonFrozenUdtField(int pk) {
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

  @Override
  String[] expectedUpdateNonFrozenNestedUdtField(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedUpdateNonFrozenUdtWithMapField(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedUpdateNonFrozenUdtWithListField(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedUpdateNonFrozenUdtWithSetField(int pk) {
    return new String[] {};
  }

  @Override
  String[] expectedUpdateFrozenUdtFromValueToEmpty(int pk) {
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

  @Override
  String[] expectedUpdateFrozenUdtFromNullToValue(int pk) {
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

  @Override
  String[] expectedUpdateFrozenUdtFromEmptyToValue(int pk) {
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

  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToValue(int pk) {
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

  @Override
  String[] expectedUpdateNonFrozenUdtFromValueToNull(int pk) {
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
