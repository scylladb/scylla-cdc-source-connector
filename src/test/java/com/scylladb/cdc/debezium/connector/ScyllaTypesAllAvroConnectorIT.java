package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

/**
 * Avro-based integration tests for all Scylla types replication. This class extends {@link
 * ScyllaTypesAllBase} and provides Avro-specific expected JSON for all test scenarios.
 */
public class ScyllaTypesAllAvroConnectorIT
    extends ScyllaTypesAllBase<GenericRecord, GenericRecord> {

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
  protected String blobValueSet1() {
    return "Êþº¾"; // Avro char encoding for 0xCAFEBABE
  }

  @Override
  protected String blobValueSet2() {
    return "Þ­¾ï"; // Avro char encoding for 0xDEADBEEF
  }

  // ===================== Helper Methods for Building Expected JSON =====================
  // Uses shared methods from ScyllaTypesAllBase for common column fragments.

  /** Returns values for all non-frozen collection columns (initial values). */
  private String nonFrozenCollectionsColumnsValues() {
    return """
        "list_col": [10, 20, 30],
        "set_col": ["x", "y", "z"],
        "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
        "map_ascii_col": [{"key": "ascii_key", "value": "ascii_value"}],
        "map_bigint_col": [{"key": 1234567890123, "value": "bigint_value"}],
        "map_boolean_col": [{"key": true, "value": "boolean_value"}],
        "map_date_col": [{"key": 19884, "value": "date_value"}],
        "map_decimal_col": [{"key": "12345.67", "value": "decimal_value"}],
        "map_double_col": [{"key": 3.14159, "value": "double_value"}],
        "map_float_col": [{"key": 2.71828, "value": "float_value"}],
        "map_int_col": [{"key": 42, "value": "int_value"}],
        "map_inet_col": [{"key": "127.0.0.1", "value": "inet_value"}],
        "map_smallint_col": [{"key": 7, "value": "smallint_value"}],
        "map_text_col": [{"key": "text_key", "value": "text_value"}],
        "map_time_col": [{"key": 45296789000000, "value": "time_value"}],
        "map_timestamp_col": [{"key": 1718022896789, "value": "timestamp_value"}],
        "map_timeuuid_col": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}],
        "map_tinyint_col": [{"key": 5, "value": "tinyint_value"}],
        "map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}],
        "map_varchar_col": [{"key": "varchar_key", "value": "varchar_value"}],
        "map_varint_col": [{"key": "999999999", "value": "varint_value"}],
        "set_timeuuid_col": ["81d4a030-4632-11f0-9484-409dd8f36eba"]""";
  }

  /** Returns updated values for all non-frozen collection columns. */
  private String nonFrozenCollectionsColumnsValuesUpdated() {
    return """
        "list_col": [40, 50, 60],
        "set_col": ["a", "b", "c"],
        "map_col": [{"key": 30, "value": "thirty"}, {"key": 40, "value": "forty"}],
        "map_ascii_col": [{"key": "ascii_key_2", "value": "ascii_value_2"}],
        "map_bigint_col": [{"key": 1234567890124, "value": "bigint_value_2"}],
        "map_boolean_col": [{"key": false, "value": "boolean_value_2"}],
        "map_date_col": [{"key": 19885, "value": "date_value_2"}],
        "map_decimal_col": [{"key": "98765.43", "value": "decimal_value_2"}],
        "map_double_col": [{"key": 2.71828, "value": "double_value_2"}],
        "map_float_col": [{"key": 1.41421, "value": "float_value_2"}],
        "map_int_col": [{"key": 43, "value": "int_value_2"}],
        "map_inet_col": [{"key": "127.0.0.2", "value": "inet_value_2"}],
        "map_smallint_col": [{"key": 8, "value": "smallint_value_2"}],
        "map_text_col": [{"key": "text_key_2", "value": "text_value_2"}],
        "map_time_col": [{"key": 3723456000000, "value": "time_value_2"}],
        "map_timestamp_col": [{"key": 1718067723456, "value": "timestamp_value_2"}],
        "map_timeuuid_col": [{"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value_2"}],
        "map_tinyint_col": [{"key": 6, "value": "tinyint_value_2"}],
        "map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371d", "value": "uuid_value_2"}],
        "map_varchar_col": [{"key": "varchar_key_2", "value": "varchar_value_2"}],
        "map_varint_col": [{"key": "888888888", "value": "varint_value_2"}],
        "set_timeuuid_col": ["81d4a031-4632-11f0-9484-409dd8f36eba"]""";
  }

  // udtColumnsNull() and udtColumnsValues() are inherited from ScyllaTypesAllBase

  /** Returns null values for primitive columns. */
  private String primitiveColumnsNull(int pk) {
    return """
        "id": %d,
        "ascii_col": null,
        "bigint_col": null,
        "blob_col": null,
        "boolean_col": null,
        "date_col": null,
        "decimal_col": null,
        "double_col": null,
        "duration_col": null,
        "float_col": null,
        "inet_col": null,
        "int_col": null,
        "smallint_col": null,
        "text_col": null,
        "time_col": null,
        "timestamp_col": null,
        "timeuuid_col": null,
        "tinyint_col": null,
        "uuid_col": null,
        "varchar_col": null,
        "varint_col": null,
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns values for primitive columns (set 1). */
  private String primitiveColumnsValues(int pk, String textValue) {
    return """
        "id": %d,
        "ascii_col": "ascii",
        "bigint_col": 1234567890123,
        "blob_col": "Êþº¾",
        "boolean_col": true,
        "date_col": 19884,
        "decimal_col": "12345.67",
        "double_col": 3.14159,
        "duration_col": "1d12h30m",
        "float_col": 2.71828,
        "inet_col": "127.0.0.1",
        "int_col": 42,
        "smallint_col": 7,
        "text_col": "%s",
        "time_col": 45296789000000,
        "timestamp_col": 1718022896789,
        "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
        "tinyint_col": 5,
        "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
        "varchar_col": "varchar text",
        "varint_col": "999999999",
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            textValue,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns values for primitive columns (set 2 - updated values). */
  private String primitiveColumnsValuesSet2(int pk, String textValue) {
    return """
        "id": %d,
        "ascii_col": "ascii2",
        "bigint_col": 1234567890124,
        "blob_col": "Þ­¾ï",
        "boolean_col": false,
        "date_col": 19885,
        "decimal_col": "98765.43",
        "double_col": 2.71828,
        "duration_col": "2d1h",
        "float_col": 1.41421,
        "inet_col": "127.0.0.2",
        "int_col": 43,
        "smallint_col": 8,
        "text_col": "%s",
        "time_col": 3723456000000,
        "timestamp_col": 1718067723456,
        "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
        "tinyint_col": 6,
        "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
        "varchar_col": "varchar text 2",
        "varint_col": "888888888",
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            textValue,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns empty string values for primitive columns. */
  private String primitiveColumnsEmpty(int pk) {
    return """
        "id": %d,
        "ascii_col": "",
        "bigint_col": 1234567890123,
        "blob_col": "Êþº¾",
        "boolean_col": true,
        "date_col": 19884,
        "decimal_col": "12345.67",
        "double_col": 3.14159,
        "duration_col": "1d12h30m",
        "float_col": 2.71828,
        "inet_col": "127.0.0.1",
        "int_col": 42,
        "smallint_col": 7,
        "text_col": "",
        "time_col": 45296789000000,
        "timestamp_col": 1718022896789,
        "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba",
        "tinyint_col": 5,
        "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c",
        "varchar_col": "",
        "varint_col": "999999999",
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  /** Returns empty string values for primitive columns (set 2). */
  private String primitiveColumnsEmptySet2(int pk) {
    return """
        "id": %d,
        "ascii_col": "",
        "bigint_col": 1234567890124,
        "blob_col": "Þ­¾ï",
        "boolean_col": false,
        "date_col": 19885,
        "decimal_col": "98765.43",
        "double_col": 2.71828,
        "duration_col": "2d1h",
        "float_col": 1.41421,
        "inet_col": "127.0.0.2",
        "int_col": 43,
        "smallint_col": 8,
        "text_col": "",
        "time_col": 3723456000000,
        "timestamp_col": 1718067723456,
        "timeuuid_col": "81d4a031-4632-11f0-9484-409dd8f36eba",
        "tinyint_col": 6,
        "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371d",
        "varchar_col": "",
        "varint_col": "888888888",
        "untouched_text": "%s",
        "untouched_int": %d,
        "untouched_boolean": %s,
        "untouched_uuid": "%s\""""
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE);
  }

  // ===================== Primitive Expected Methods =====================

  @Override
  String[] expectedPrimitiveInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "some text"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "delete me"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // DELETE record
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
      null
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToNil(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsEmptySet2(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsValuesSet2(pk, "value2"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsValues(pk, "value"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToEmpty(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsEmptySet2(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToNil(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsValuesSet2(pk, "value2"),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToNil(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsNull(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              primitiveColumnsEmpty(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              primitiveColumnsEmptySet2(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  // ===================== Frozen Collections Expected Methods =====================

  /** Helper for frozen collections - returns after JSON with all columns. */
  private String frozenAfterColumnsWithValues(int pk) {
    return """
        "id": %d,
        "ascii_col": null,
        "bigint_col": null,
        "blob_col": null,
        "boolean_col": null,
        "date_col": null,
        "decimal_col": null,
        "double_col": null,
        "duration_col": null,
        "float_col": null,
        "inet_col": null,
        "int_col": null,
        "smallint_col": null,
        "text_col": null,
        "time_col": null,
        "timestamp_col": null,
        "timeuuid_col": null,
        "tinyint_col": null,
        "uuid_col": null,
        "varchar_col": null,
        "varint_col": null,
        "untouched_text": null,
        "untouched_int": null,
        "untouched_boolean": null,
        "untouched_uuid": null"""
        .formatted(pk);
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithEmpty(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsEmpty(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // DELETE record
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
      null
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValuesUpdated(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsEmpty(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsEmpty(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsEmpty(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsValues(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  // ===================== Non-Frozen Collections Expected Methods =====================

  @Override
  String[] expectedNonFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": [10],
            "set_col": ["x"],
            "map_col": [{"key": 10, "value": "ten"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // DELETE record
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
      null
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListAddElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": [10, 20],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": [10, 20],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": [10, 20, 30],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetAddElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "y"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "y"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "y", "z"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapAddElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 10, "value": "ten"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 10, "value": "ten"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListRemoveElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": [10, 20, 30],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": [10, 20, 30],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": [10, 30],
            "set_col": null,
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetRemoveElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "y", "z"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "y", "z"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": ["x", "z"],
            "map_col": null,
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapRemoveElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "after": {
            %s,
            %s,
            "list_col": null,
            "set_col": null,
            "map_col": [{"key": 20, "value": "twenty"}],
            "map_ascii_col": null,
            "map_bigint_col": null,
            "map_boolean_col": null,
            "map_date_col": null,
            "map_decimal_col": null,
            "map_double_col": null,
            "map_float_col": null,
            "map_int_col": null,
            "map_inet_col": null,
            "map_smallint_col": null,
            "map_text_col": null,
            "map_time_col": null,
            "map_timestamp_col": null,
            "map_timeuuid_col": null,
            "map_tinyint_col": null,
            "map_uuid_col": null,
            "map_varchar_col": null,
            "map_varint_col": null,
            "set_timeuuid_col": null,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValuesUpdated(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    // Empty non-frozen collections become null in Scylla
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    // Empty non-frozen collections are stored as null in Scylla
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsValues(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  // ===================== UDT Expected Methods =====================

  @Override
  String[] expectedUdtInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtInsertWithEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource()),
      // DELETE record
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
      null
    };
  }

  /** UDT values for frozen update test (nf_udt has same value as frozen_udt). */
  private String udtColumnsValuesForFrozenUpdate() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": {"a": 42, "b": "foo"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  /** UDT values after frozen update. */
  private String udtColumnsValuesAfterFrozenUpdate() {
    return """
        "frozen_udt": {"a": 99, "b": "updated"},
        "nf_udt": {"a": 77, "b": "foo"},
        "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "frozen_udt_with_list": {"l": [4, 5, 6]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["d", "e"]},
        "nf_udt_with_set": {"s": ["d", "e"]}""";
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForFrozenUpdate(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForFrozenUpdate(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesAfterFrozenUpdate(),
              pk,
              expectedSource())
    };
  }

  /** UDT values for value-to-null test (nf_udt is null). */
  private String udtColumnsValuesForValueToNull() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": null,
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForValueToNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForValueToNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromNullToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromEmptyToValue(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsNull(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource())
    };
  }

  /** UDT values for non-frozen field test (frozen_udt is null). */
  private String udtColumnsValuesForNonFrozenField() {
    return """
        "frozen_udt": null,
        "nf_udt": {"a": 7, "b": "bar"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [1, 2, 3]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["a", "b", "c"]}""";
  }

  /** UDT values after non-frozen field update. */
  private String udtColumnsValuesAfterNonFrozenFieldUpdate() {
    return """
        "frozen_udt": {"a": 99, "b": "updated"},
        "nf_udt": {"a": 100, "b": "bar"},
        "frozen_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "nf_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "updated1"}, {"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "frozen_udt_with_list": {"l": [4, 5, 6]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["d", "e"]},
        "nf_udt_with_set": {"s": ["d", "e"]}""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtField(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForNonFrozenField(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesForNonFrozenField(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesAfterNonFrozenFieldUpdate(),
              pk,
              expectedSource())
    };
  }

  /** UDT values after non-frozen replace update. */
  private String udtColumnsValuesAfterNonFrozenReplace() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": {"a": 99, "b": "updated"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 11, "y": "updated"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}, {"key": "81d4a033-4632-11f0-9484-409dd8f36eba", "value": "value4"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [4, 5, 6]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["d", "e", "f"]}""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesAfterNonFrozenReplace(),
              pk,
              expectedSource())
    };
  }

  /** UDT values after non-frozen columns set to null. */
  private String udtColumnsValuesAfterNonFrozenToNull() {
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": null,
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": null,
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": null,
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": null,
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": null""";
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "before": null,
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              pk,
              expectedSource()),
      // UPDATE record
      """
        {
          "before": {
            %s,
            %s,
            %s,
            %s
          },
          "after": {
            %s,
            %s,
            %s,
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValues(),
              frozenAfterColumnsWithValues(pk),
              frozenCollectionsColumnsNull(),
              nonFrozenCollectionsColumnsNull(),
              udtColumnsValuesAfterNonFrozenToNull(),
              pk,
              expectedSource())
    };
  }
}
