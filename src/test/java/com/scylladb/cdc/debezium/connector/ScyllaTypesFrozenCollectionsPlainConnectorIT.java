package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
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
  String[] expectedInsertWithValues(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_list_col": {"value": [1, 2, 3]},
            "frozen_set_col": {"value": ["a", "b", "c"]},
            "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
            "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
            "frozen_map_ascii_col": {"value": {"ascii_key": "ascii_value"}},
            "frozen_map_bigint_col": {"value": [[1234567890123, "bigint_value"]]},
            "frozen_map_boolean_col": {"value": [[true, "boolean_value"]]},
            "frozen_map_date_col": {"value": [[19884, "date_value"]]},
            "frozen_map_decimal_col": {"value": {"12345.67": "decimal_value"}},
            "frozen_map_double_col": {"value": [[3.14159, "double_value"]]},
            "frozen_map_float_col": {"value": [[2.71828, "float_value"]]},
            "frozen_map_int_col": {"value": [[42, "int_value"]]},
            "frozen_map_inet_col": {"value": {"127.0.0.1": "inet_value"}},
            "frozen_map_smallint_col": {"value": [[7, "smallint_value"]]},
            "frozen_map_text_col": {"value": {"text_key": "text_value"}},
            "frozen_map_time_col": {"value": [[45296789000000, "time_value"]]},
            "frozen_map_timestamp_col": {"value": [[1718022896789, "timestamp_value"]]},
            "frozen_map_timeuuid_col": {"value": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"}},
            "frozen_map_tinyint_col": {"value": [[5, "tinyint_value"]]},
            "frozen_map_uuid_col": {"value": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"}},
            "frozen_map_varchar_col": {"value": {"varchar_key": "varchar_value"}},
            "frozen_map_varint_col": {"value": {"999999999": "varint_value"}},
            "frozen_map_tuple_key_col": {"value": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]]},
            "frozen_map_udt_key_col": {"value": [[{"a": 1, "b": "udt_key"}, "udt_value"]]}
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
  String[] expectedInsertWithEmpty(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_list_col": {"value": []},
            "frozen_set_col": {"value": []},
            "frozen_map_col": {"value": []},
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}},
            "frozen_map_ascii_col": {"value": {}},
            "frozen_map_bigint_col": {"value": []},
            "frozen_map_boolean_col": {"value": []},
            "frozen_map_date_col": {"value": []},
            "frozen_map_decimal_col": {"value": {}},
            "frozen_map_double_col": {"value": []},
            "frozen_map_float_col": {"value": []},
            "frozen_map_int_col": {"value": []},
            "frozen_map_inet_col": {"value": {}},
            "frozen_map_smallint_col": {"value": []},
            "frozen_map_text_col": {"value": {}},
            "frozen_map_time_col": {"value": []},
            "frozen_map_timestamp_col": {"value": []},
            "frozen_map_timeuuid_col": {"value": {}},
            "frozen_map_tinyint_col": {"value": []},
            "frozen_map_uuid_col": {"value": {}},
            "frozen_map_varchar_col": {"value": {}},
            "frozen_map_varint_col": {"value": {}},
            "frozen_map_tuple_key_col": {"value": []},
            "frozen_map_udt_key_col": {"value": []}
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
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "frozen_list_col": {"value": null},
            "frozen_set_col": {"value": null},
            "frozen_map_col": {"value": null},
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}},
            "frozen_map_ascii_col": {"value": null},
            "frozen_map_bigint_col": {"value": null},
            "frozen_map_boolean_col": {"value": null},
            "frozen_map_date_col": {"value": null},
            "frozen_map_decimal_col": {"value": null},
            "frozen_map_double_col": {"value": null},
            "frozen_map_float_col": {"value": null},
            "frozen_map_int_col": {"value": null},
            "frozen_map_inet_col": {"value": null},
            "frozen_map_smallint_col": {"value": null},
            "frozen_map_text_col": {"value": null},
            "frozen_map_time_col": {"value": null},
            "frozen_map_timestamp_col": {"value": null},
            "frozen_map_timeuuid_col": {"value": null},
            "frozen_map_tinyint_col": {"value": null},
            "frozen_map_uuid_col": {"value": null},
            "frozen_map_varchar_col": {"value": null},
            "frozen_map_varint_col": {"value": null},
            "frozen_map_tuple_key_col": {"value": null},
            "frozen_map_udt_key_col": {"value": null}
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
  String[] expectedDelete(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "d",
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
              .formatted(pk),
          "null"),
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_list_col": {"value": [4, 5, 6]},
              "frozen_set_col": {"value": ["x", "y", "z"]},
              "frozen_map_col": {"value": [[3, "three"], [4, "four"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 99, "tuple_member_1": "bar"}},
              "frozen_map_ascii_col": {"value": {"ascii_key_2": "ascii_value_2"}},
              "frozen_map_bigint_col": {"value": [[1234567890124, "bigint_value_2"]]},
              "frozen_map_boolean_col": {"value": [[false, "boolean_value_2"]]},
              "frozen_map_date_col": {"value": [[19885, "date_value_2"]]},
              "frozen_map_decimal_col": {"value": {"98765.43": "decimal_value_2"}},
              "frozen_map_double_col": {"value": [[2.71828, "double_value_2"]]},
              "frozen_map_float_col": {"value": [[1.41421, "float_value_2"]]},
              "frozen_map_int_col": {"value": [[43, "int_value_2"]]},
              "frozen_map_inet_col": {"value": {"127.0.0.2": "inet_value_2"}},
              "frozen_map_smallint_col": {"value": [[8, "smallint_value_2"]]},
              "frozen_map_text_col": {"value": {"text_key_2": "text_value_2"}},
              "frozen_map_time_col": {"value": [[3723456000000, "time_value_2"]]},
              "frozen_map_timestamp_col": {"value": [[1718067723456, "timestamp_value_2"]]},
              "frozen_map_timeuuid_col": {"value": {"81d4a031-4632-11f0-9484-409dd8f36eba": "timeuuid_value_2"}},
              "frozen_map_tinyint_col": {"value": [[6, "tinyint_value_2"]]},
              "frozen_map_uuid_col": {"value": {"453662fa-db4b-4938-9033-d8523c0a371d": "uuid_value_2"}},
              "frozen_map_varchar_col": {"value": {"varchar_key_2": "varchar_value_2"}},
              "frozen_map_varint_col": {"value": {"888888888": "varint_value_2"}},
              "frozen_map_tuple_key_col": {"value": [[{"tuple_member_0": 2, "tuple_member_1": "tuple_key_2"}, "tuple_value_2"]]},
              "frozen_map_udt_key_col": {"value": [[{"a": 2, "b": "udt_key_2"}, "udt_value_2"]]}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_list_col": {"value": []},
              "frozen_set_col": {"value": []},
              "frozen_map_col": {"value": []},
              "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}},
              "frozen_map_ascii_col": {"value": {}},
              "frozen_map_bigint_col": {"value": []},
              "frozen_map_boolean_col": {"value": []},
              "frozen_map_date_col": {"value": []},
              "frozen_map_decimal_col": {"value": {}},
              "frozen_map_double_col": {"value": []},
              "frozen_map_float_col": {"value": []},
              "frozen_map_int_col": {"value": []},
              "frozen_map_inet_col": {"value": {}},
              "frozen_map_smallint_col": {"value": []},
              "frozen_map_text_col": {"value": {}},
              "frozen_map_time_col": {"value": []},
              "frozen_map_timestamp_col": {"value": []},
              "frozen_map_timeuuid_col": {"value": {}},
              "frozen_map_tinyint_col": {"value": []},
              "frozen_map_uuid_col": {"value": {}},
              "frozen_map_varchar_col": {"value": {}},
              "frozen_map_varint_col": {"value": {}},
              "frozen_map_tuple_key_col": {"value": []},
              "frozen_map_udt_key_col": {"value": []}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_list_col": {"value": null},
              "frozen_set_col": {"value": null},
              "frozen_map_col": {"value": null},
              "frozen_tuple_col": {"value": null},
              "frozen_map_ascii_col": {"value": null},
              "frozen_map_bigint_col": {"value": null},
              "frozen_map_boolean_col": {"value": null},
              "frozen_map_date_col": {"value": null},
              "frozen_map_decimal_col": {"value": null},
              "frozen_map_double_col": {"value": null},
              "frozen_map_float_col": {"value": null},
              "frozen_map_int_col": {"value": null},
              "frozen_map_inet_col": {"value": null},
              "frozen_map_smallint_col": {"value": null},
              "frozen_map_text_col": {"value": null},
              "frozen_map_time_col": {"value": null},
              "frozen_map_timestamp_col": {"value": null},
              "frozen_map_timeuuid_col": {"value": null},
              "frozen_map_tinyint_col": {"value": null},
              "frozen_map_uuid_col": {"value": null},
              "frozen_map_varchar_col": {"value": null},
              "frozen_map_varint_col": {"value": null},
              "frozen_map_tuple_key_col": {"value": null},
              "frozen_map_udt_key_col": {"value": null}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
              "frozen_map_ascii_col": {"value": {"ascii_key": "ascii_value"}},
              "frozen_map_bigint_col": {"value": [[1234567890123, "bigint_value"]]},
              "frozen_map_boolean_col": {"value": [[true, "boolean_value"]]},
              "frozen_map_date_col": {"value": [[19884, "date_value"]]},
              "frozen_map_decimal_col": {"value": {"12345.67": "decimal_value"}},
              "frozen_map_double_col": {"value": [[3.14159, "double_value"]]},
              "frozen_map_float_col": {"value": [[2.71828, "float_value"]]},
              "frozen_map_int_col": {"value": [[42, "int_value"]]},
              "frozen_map_inet_col": {"value": {"127.0.0.1": "inet_value"}},
              "frozen_map_smallint_col": {"value": [[7, "smallint_value"]]},
              "frozen_map_text_col": {"value": {"text_key": "text_value"}},
              "frozen_map_time_col": {"value": [[45296789000000, "time_value"]]},
              "frozen_map_timestamp_col": {"value": [[1718022896789, "timestamp_value"]]},
              "frozen_map_timeuuid_col": {"value": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"}},
              "frozen_map_tinyint_col": {"value": [[5, "tinyint_value"]]},
              "frozen_map_uuid_col": {"value": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"}},
              "frozen_map_varchar_col": {"value": {"varchar_key": "varchar_value"}},
              "frozen_map_varint_col": {"value": {"999999999": "varint_value"}},
              "frozen_map_tuple_key_col": {"value": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]]},
              "frozen_map_udt_key_col": {"value": [[{"a": 1, "b": "udt_key"}, "udt_value"]]}
            }
            """
              .formatted(pk))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromNullToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": %d,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [[1, "one"], [2, "two"]]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
              "frozen_map_ascii_col": {"value": {"ascii_key": "ascii_value"}},
              "frozen_map_bigint_col": {"value": [[1234567890123, "bigint_value"]]},
              "frozen_map_boolean_col": {"value": [[true, "boolean_value"]]},
              "frozen_map_date_col": {"value": [[19884, "date_value"]]},
              "frozen_map_decimal_col": {"value": {"12345.67": "decimal_value"}},
              "frozen_map_double_col": {"value": [[3.14159, "double_value"]]},
              "frozen_map_float_col": {"value": [[2.71828, "float_value"]]},
              "frozen_map_int_col": {"value": [[42, "int_value"]]},
              "frozen_map_inet_col": {"value": {"127.0.0.1": "inet_value"}},
              "frozen_map_smallint_col": {"value": [[7, "smallint_value"]]},
              "frozen_map_text_col": {"value": {"text_key": "text_value"}},
              "frozen_map_time_col": {"value": [[45296789000000, "time_value"]]},
              "frozen_map_timestamp_col": {"value": [[1718022896789, "timestamp_value"]]},
              "frozen_map_timeuuid_col": {"value": {"81d4a030-4632-11f0-9484-409dd8f36eba": "timeuuid_value"}},
              "frozen_map_tinyint_col": {"value": [[5, "tinyint_value"]]},
              "frozen_map_uuid_col": {"value": {"453662fa-db4b-4938-9033-d8523c0a371c": "uuid_value"}},
              "frozen_map_varchar_col": {"value": {"varchar_key": "varchar_value"}},
              "frozen_map_varint_col": {"value": {"999999999": "varint_value"}},
              "frozen_map_tuple_key_col": {"value": [[{"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "tuple_value"]]},
              "frozen_map_udt_key_col": {"value": [[{"a": 1, "b": "udt_key"}, "udt_value"]]}
            }
            """
              .formatted(pk))
    };
  }
}
