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
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
          "frozen_map_ascii_col": {"ascii_key": "ascii_value"},
          "frozen_map_bigint_col": [[1234567890123, "bigint_value"]],
          "frozen_map_boolean_col": [[true, "boolean_value"]],
          "frozen_map_date_col": [[19884, "date_value"]],
          "frozen_map_decimal_col": {"12345.67": "decimal_value"},
          "frozen_map_double_col": [[3.14159, "double_value"]],
          "frozen_map_float_col": [[2.71828, "float_value"]],
          "frozen_map_int_col": [[42, "int_value"]],
          "frozen_map_inet_col": {"127.0.0.1": "inet_value"},
          "frozen_map_smallint_col": [[7, "smallint_value"]],
          "frozen_map_text_col": {"text_key": "text_value"},
          "frozen_map_time_col": [[45296789000000, "time_value"]],
          "frozen_map_timestamp_col": [[1718022896789, "timestamp_value"]],
          "frozen_map_timeuuid_col": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"},
          "frozen_map_tinyint_col": [[5, "tinyint_value"]],
          "frozen_map_uuid_col": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"},
          "frozen_map_varchar_col": {"varchar_key": "varchar_value"},
          "frozen_map_varint_col": {"999999999": "varint_value"},
          "frozen_map_tuple_key_col": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]],
          "frozen_map_udt_key_col": [[{"a": 1, "b": "udt_key"}, "udt_value"]]
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
          "frozen_map_ascii_col": {},
          "frozen_map_bigint_col": [],
          "frozen_map_boolean_col": [],
          "frozen_map_date_col": [],
          "frozen_map_decimal_col": {},
          "frozen_map_double_col": [],
          "frozen_map_float_col": [],
          "frozen_map_int_col": [],
          "frozen_map_inet_col": {},
          "frozen_map_smallint_col": [],
          "frozen_map_text_col": {},
          "frozen_map_time_col": [],
          "frozen_map_timestamp_col": [],
          "frozen_map_timeuuid_col": {},
          "frozen_map_tinyint_col": [],
          "frozen_map_uuid_col": {},
          "frozen_map_varchar_col": {},
          "frozen_map_varint_col": {},
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
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
          "frozen_map_ascii_col": {"ascii_key": "ascii_value"},
          "frozen_map_bigint_col": [[1234567890123, "bigint_value"]],
          "frozen_map_boolean_col": [[true, "boolean_value"]],
          "frozen_map_date_col": [[19884, "date_value"]],
          "frozen_map_decimal_col": {"12345.67": "decimal_value"},
          "frozen_map_double_col": [[3.14159, "double_value"]],
          "frozen_map_float_col": [[2.71828, "float_value"]],
          "frozen_map_int_col": [[42, "int_value"]],
          "frozen_map_inet_col": {"127.0.0.1": "inet_value"},
          "frozen_map_smallint_col": [[7, "smallint_value"]],
          "frozen_map_text_col": {"text_key": "text_value"},
          "frozen_map_time_col": [[45296789000000, "time_value"]],
          "frozen_map_timestamp_col": [[1718022896789, "timestamp_value"]],
          "frozen_map_timeuuid_col": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"},
          "frozen_map_tinyint_col": [[5, "tinyint_value"]],
          "frozen_map_uuid_col": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"},
          "frozen_map_varchar_col": {"varchar_key": "varchar_value"},
          "frozen_map_varint_col": {"999999999": "varint_value"},
          "frozen_map_tuple_key_col": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]],
          "frozen_map_udt_key_col": [[{"a": 1, "b": "udt_key"}, "udt_value"]]
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
          "frozen_map_col": [[3, "three"], [4, "four"]],
          "frozen_tuple_col": {"tuple_member_0": 99, "tuple_member_1": "bar"},
          "frozen_map_ascii_col": {"ascii_key_2": "ascii_value_2"},
          "frozen_map_bigint_col": [[1234567890124, "bigint_value_2"]],
          "frozen_map_boolean_col": [[false, "boolean_value_2"]],
          "frozen_map_date_col": [[19885, "date_value_2"]],
          "frozen_map_decimal_col": {"98765.43": "decimal_value_2"},
          "frozen_map_double_col": [[2.71828, "double_value_2"]],
          "frozen_map_float_col": [[1.41421, "float_value_2"]],
          "frozen_map_int_col": [[43, "int_value_2"]],
          "frozen_map_inet_col": {"127.0.0.2": "inet_value_2"},
          "frozen_map_smallint_col": [[8, "smallint_value_2"]],
          "frozen_map_text_col": {"text_key_2": "text_value_2"},
          "frozen_map_time_col": [[3723456000000, "time_value_2"]],
          "frozen_map_timestamp_col": [[1718067723456, "timestamp_value_2"]],
          "frozen_map_timeuuid_col": {"81d4a031-4632-11f0-9484-409dd8f36eba": "timeuuid_value_2"},
          "frozen_map_tinyint_col": [[6, "tinyint_value_2"]],
          "frozen_map_uuid_col": {"453662fa-db4b-4938-9033-d8523c0a371d": "uuid_value_2"},
          "frozen_map_varchar_col": {"varchar_key_2": "varchar_value_2"},
          "frozen_map_varint_col": {"888888888": "varint_value_2"},
          "frozen_map_tuple_key_col": [[{"tuple_member_0": 2, "tuple_member_1": "tuple_key_2"}, "tuple_value_2"]],
          "frozen_map_udt_key_col": [[{"a": 2, "b": "udt_key_2"}, "udt_value_2"]]
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
          "frozen_map_ascii_col": {},
          "frozen_map_bigint_col": [],
          "frozen_map_boolean_col": [],
          "frozen_map_date_col": [],
          "frozen_map_decimal_col": {},
          "frozen_map_double_col": [],
          "frozen_map_float_col": [],
          "frozen_map_int_col": [],
          "frozen_map_inet_col": {},
          "frozen_map_smallint_col": [],
          "frozen_map_text_col": {},
          "frozen_map_time_col": [],
          "frozen_map_timestamp_col": [],
          "frozen_map_timeuuid_col": {},
          "frozen_map_tinyint_col": [],
          "frozen_map_uuid_col": {},
          "frozen_map_varchar_col": {},
          "frozen_map_varint_col": {},
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
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
          "frozen_map_ascii_col": {"ascii_key": "ascii_value"},
          "frozen_map_bigint_col": [[1234567890123, "bigint_value"]],
          "frozen_map_boolean_col": [[true, "boolean_value"]],
          "frozen_map_date_col": [[19884, "date_value"]],
          "frozen_map_decimal_col": {"12345.67": "decimal_value"},
          "frozen_map_double_col": [[3.14159, "double_value"]],
          "frozen_map_float_col": [[2.71828, "float_value"]],
          "frozen_map_int_col": [[42, "int_value"]],
          "frozen_map_inet_col": {"127.0.0.1": "inet_value"},
          "frozen_map_smallint_col": [[7, "smallint_value"]],
          "frozen_map_text_col": {"text_key": "text_value"},
          "frozen_map_time_col": [[45296789000000, "time_value"]],
          "frozen_map_timestamp_col": [[1718022896789, "timestamp_value"]],
          "frozen_map_timeuuid_col": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"},
          "frozen_map_tinyint_col": [[5, "tinyint_value"]],
          "frozen_map_uuid_col": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"},
          "frozen_map_varchar_col": {"varchar_key": "varchar_value"},
          "frozen_map_varint_col": {"999999999": "varint_value"},
          "frozen_map_tuple_key_col": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]],
          "frozen_map_udt_key_col": [[{"a": 1, "b": "udt_key"}, "udt_value"]]
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
          "frozen_map_col": [[1, "one"], [2, "two"]],
          "frozen_tuple_col": {"tuple_member_0": 42, "tuple_member_1": "foo"},
          "frozen_map_ascii_col": {"ascii_key": "ascii_value"},
          "frozen_map_bigint_col": [[1234567890123, "bigint_value"]],
          "frozen_map_boolean_col": [[true, "boolean_value"]],
          "frozen_map_date_col": [[19884, "date_value"]],
          "frozen_map_decimal_col": {"12345.67": "decimal_value"},
          "frozen_map_double_col": [[3.14159, "double_value"]],
          "frozen_map_float_col": [[2.71828, "float_value"]],
          "frozen_map_int_col": [[42, "int_value"]],
          "frozen_map_inet_col": {"127.0.0.1": "inet_value"},
          "frozen_map_smallint_col": [[7, "smallint_value"]],
          "frozen_map_text_col": {"text_key": "text_value"},
          "frozen_map_time_col": [[45296789000000, "time_value"]],
          "frozen_map_timestamp_col": [[1718022896789, "timestamp_value"]],
          "frozen_map_timeuuid_col": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"},
          "frozen_map_tinyint_col": [[5, "tinyint_value"]],
          "frozen_map_uuid_col": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"},
          "frozen_map_varchar_col": {"varchar_key": "varchar_value"},
          "frozen_map_varint_col": {"999999999": "varint_value"},
          "frozen_map_tuple_key_col": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]],
          "frozen_map_udt_key_col": [[{"a": 1, "b": "udt_key"}, "udt_value"]]
        }
        """
          .formatted(pk)
    };
  }
}
