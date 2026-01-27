package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
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
  String[] expectedInsertWithValues(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
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
          "frozen_map_tuple_key_col": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}],
          "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithEmpty(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "frozen_list_col": [],
          "frozen_set_col": [],
          "frozen_map_col": [],
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null},
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
          "frozen_map_udt_key_col": []
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
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null,
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null},
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
          "frozen_map_udt_key_col": null
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
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
          "frozen_map_tuple_key_col": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}],
          "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_list_col": [4, 5, 6],
          "frozen_set_col": ["x", "y", "z"],
          "frozen_map_col": [{"key": 3, "value": "three"}, {"key": 4, "value": "four"}],
          "frozen_tuple_col": {"tuple_member_0": 99, "tuple_member_1": "bar"},
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
          "frozen_map_tuple_key_col": [{"key": {"tuple_member_0": 2, "tuple_member_1": "tuple_key_2"}, "value": "tuple_value_2"}],
          "frozen_map_udt_key_col": [{"key": {"a": 2, "b": "udt_key_2"}, "value": "udt_value_2"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_list_col": [],
          "frozen_set_col": [],
          "frozen_map_col": [],
          "frozen_tuple_col": {"tuple_member_0": null, "tuple_member_1": null},
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
          "frozen_map_udt_key_col": []
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToNull(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
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
          "frozen_map_udt_key_col": null
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
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
          "frozen_map_tuple_key_col": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}],
          "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]
        }
        """
          .formatted(pk)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromNullToValue(int pk) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": %d,
          "frozen_list_col": [1, 2, 3],
          "frozen_set_col": ["a", "b", "c"],
          "frozen_map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
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
          "frozen_map_tuple_key_col": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}],
          "frozen_map_udt_key_col": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]
        }
        """
          .formatted(pk)
    };
  }
}
