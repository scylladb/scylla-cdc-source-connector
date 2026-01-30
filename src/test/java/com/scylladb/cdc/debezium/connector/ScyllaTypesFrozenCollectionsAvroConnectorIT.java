package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesFrozenCollectionsAvroConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<GenericRecord, GenericRecord> {

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

  /** Returns the full "after" JSON for an insert with values. */
  private String afterInsertWithValues(int pk) {
    return """
        {
          "id": %d,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
          "frozen_tuple_col": {"field_0": 42, "field_1": "foo"},
          "frozen_map_ascii_col": [{"key": "ascii_key", "value": "ascii_value"}],
          "frozen_map_bigint_col": [{"key": 1234567890123, "value": "bigint_value"}],
          "frozen_map_boolean_col": [{"key": true, "value": "boolean_value"}],
          "frozen_map_date_col": [{"key": 19884, "value": "date_value"}],
          "frozen_map_decimal_col": [{"key": "12345.67", "value": "decimal_value"}],
          "frozen_map_double_col": [{"key": 3.14159, "value": "double_value"}],
          "frozen_map_float_col": [{"key": 2.71828, "value": "float_value"}],
          "frozen_map_int_col": [{"key": 42, "value": "int_value"}],
          "frozen_map_inet_col": [{"key": "127.0.0.1", "value": "inet_value"}],
          "frozen_map_smallint_col": [{"key": 7, "value": "smallint_value"}],
          "frozen_map_text_col": [{"key": "text_key", "value": "text_value"}],
          "frozen_map_time_col": [{"key": 45296789000000, "value": "time_value"}],
          "frozen_map_timestamp_col": [{"key": 1718022896789, "value": "timestamp_value"}],
          "frozen_map_timeuuid_col": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}],
          "frozen_map_tinyint_col": [{"key": 5, "value": "tinyint_value"}],
          "frozen_map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}],
          "frozen_map_varchar_col": [{"key": "varchar_key", "value": "varchar_value"}],
          "frozen_map_varint_col": [{"key": "999999999", "value": "varint_value"}],
          "frozen_map_tuple_key_col": [{"key": {"field_0": 1, "field_1": "tuple_key"}, "value": "tuple_value"}],
          "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}],
          "frozen_set_timeuuid_col": ["81d4a030-4632-11f0-9484-409dd8f36eba"]
        }
        """
        .formatted(pk);
  }

  /** Returns the full "after" JSON for an update with new values. */
  private String afterUpdateWithValues(int pk) {
    return """
        {
          "id": %d,
          "frozen_list_col": [4, 5, 6],
          "frozen_set_col": ["x", "y", "z"],
          "frozen_map_col": [{"key": 3, "value": "three"}, {"key": 4, "value": "four"}],
          "frozen_tuple_col": {"field_0": 99, "field_1": "bar"},
          "frozen_map_ascii_col": [{"key": "ascii_key_2", "value": "ascii_value_2"}],
          "frozen_map_bigint_col": [{"key": 1234567890124, "value": "bigint_value_2"}],
          "frozen_map_boolean_col": [{"key": false, "value": "boolean_value_2"}],
          "frozen_map_date_col": [{"key": 19885, "value": "date_value_2"}],
          "frozen_map_decimal_col": [{"key": "98765.43", "value": "decimal_value_2"}],
          "frozen_map_double_col": [{"key": 2.71828, "value": "double_value_2"}],
          "frozen_map_float_col": [{"key": 1.41421, "value": "float_value_2"}],
          "frozen_map_int_col": [{"key": 43, "value": "int_value_2"}],
          "frozen_map_inet_col": [{"key": "127.0.0.2", "value": "inet_value_2"}],
          "frozen_map_smallint_col": [{"key": 8, "value": "smallint_value_2"}],
          "frozen_map_text_col": [{"key": "text_key_2", "value": "text_value_2"}],
          "frozen_map_time_col": [{"key": 3723456000000, "value": "time_value_2"}],
          "frozen_map_timestamp_col": [{"key": 1718067723456, "value": "timestamp_value_2"}],
          "frozen_map_timeuuid_col": [{"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value_2"}],
          "frozen_map_tinyint_col": [{"key": 6, "value": "tinyint_value_2"}],
          "frozen_map_uuid_col": [{"key": "453662fa-db4b-4938-9033-d8523c0a371d", "value": "uuid_value_2"}],
          "frozen_map_varchar_col": [{"key": "varchar_key_2", "value": "varchar_value_2"}],
          "frozen_map_varint_col": [{"key": "888888888", "value": "varint_value_2"}],
          "frozen_map_tuple_key_col": [{"key": {"field_0": 2, "field_1": "tuple_key_2"}, "value": "tuple_value_2"}],
          "frozen_map_udt_key_col": [{"key": {"a": 2, "b": "udt_key_2"}, "value": "udt_value_2"}],
          "frozen_set_timeuuid_col": ["81d4a031-4632-11f0-9484-409dd8f36eba"]
        }
        """
        .formatted(pk);
  }

  /** Returns the "after" JSON for empty collections. */
  private String afterEmpty(int pk) {
    return """
        {
          "id": %d,
          "frozen_list_col": [],
          "frozen_set_col": [],
          "frozen_map_col": [],
          "frozen_tuple_col": {"field_0": null, "field_1": null},
          "frozen_map_ascii_col": [],
          "frozen_map_bigint_col": [],
          "frozen_map_boolean_col": [],
          "frozen_map_date_col": [],
          "frozen_map_decimal_col": [],
          "frozen_map_double_col": [],
          "frozen_map_float_col": [],
          "frozen_map_int_col": [],
          "frozen_map_inet_col": [],
          "frozen_map_smallint_col": [],
          "frozen_map_text_col": [],
          "frozen_map_time_col": [],
          "frozen_map_timestamp_col": [],
          "frozen_map_timeuuid_col": [],
          "frozen_map_tinyint_col": [],
          "frozen_map_uuid_col": [],
          "frozen_map_varchar_col": [],
          "frozen_map_varint_col": [],
          "frozen_map_tuple_key_col": [],
          "frozen_map_udt_key_col": [],
          "frozen_set_timeuuid_col": []
        }
        """
        .formatted(pk);
  }

  /** Returns the "after" JSON for null collections (when inserting with explicit null tuple). */
  private String afterNull(int pk) {
    return """
        {
          "id": %d,
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null,
          "frozen_tuple_col": null,
          "frozen_map_ascii_col": null,
          "frozen_map_bigint_col": null,
          "frozen_map_boolean_col": null,
          "frozen_map_date_col": null,
          "frozen_map_decimal_col": null,
          "frozen_map_double_col": null,
          "frozen_map_float_col": null,
          "frozen_map_int_col": null,
          "frozen_map_inet_col": null,
          "frozen_map_smallint_col": null,
          "frozen_map_text_col": null,
          "frozen_map_time_col": null,
          "frozen_map_timestamp_col": null,
          "frozen_map_timeuuid_col": null,
          "frozen_map_tinyint_col": null,
          "frozen_map_uuid_col": null,
          "frozen_map_varchar_col": null,
          "frozen_map_varint_col": null,
          "frozen_map_tuple_key_col": null,
          "frozen_map_udt_key_col": null,
          "frozen_set_timeuuid_col": null
        }
        """
        .formatted(pk);
  }

  /** Returns the "after" JSON for completely null collections (when inserting only pk). */
  private String afterAllNull(int pk) {
    return """
        {
          "id": %d,
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null,
          "frozen_tuple_col": null,
          "frozen_map_ascii_col": null,
          "frozen_map_bigint_col": null,
          "frozen_map_boolean_col": null,
          "frozen_map_date_col": null,
          "frozen_map_decimal_col": null,
          "frozen_map_double_col": null,
          "frozen_map_float_col": null,
          "frozen_map_int_col": null,
          "frozen_map_inet_col": null,
          "frozen_map_smallint_col": null,
          "frozen_map_text_col": null,
          "frozen_map_time_col": null,
          "frozen_map_timestamp_col": null,
          "frozen_map_timeuuid_col": null,
          "frozen_map_tinyint_col": null,
          "frozen_map_uuid_col": null,
          "frozen_map_varchar_col": null,
          "frozen_map_varint_col": null,
          "frozen_map_tuple_key_col": null,
          "frozen_map_udt_key_col": null,
          "frozen_set_timeuuid_col": null
        }
        """
        .formatted(pk);
  }

  /** Returns the expected key JSON. */
  private String expectedKeyFor(int pk) {
    return "{\"id\": %d}".formatted(pk);
  }

  @Override
  String[] expectedInsertWithValues(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedInsertWithEmpty(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterEmpty(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterNull(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": null,
          "after": null,
          "key": %s,
          "op": "d",
          "source": %s
        }
        """
          .formatted(expectedKeyFor(pk), expectedSource()),
      null
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              afterInsertWithValues(pk),
              afterUpdateWithValues(pk),
              expectedKeyFor(pk),
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              afterInsertWithValues(pk), afterEmpty(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              afterInsertWithValues(pk), afterAllNull(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterEmpty(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              afterEmpty(pk), afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNullToValue(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": %s,
          "key": %s,
          "op": "c",
          "source": %s
        }
        """
          .formatted(afterAllNull(pk), expectedKeyFor(pk), expectedSource()),
      """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "u",
          "source": %s
        }
        """
          .formatted(
              afterAllNull(pk), afterInsertWithValues(pk), expectedKeyFor(pk), expectedSource())
    };
  }
}
