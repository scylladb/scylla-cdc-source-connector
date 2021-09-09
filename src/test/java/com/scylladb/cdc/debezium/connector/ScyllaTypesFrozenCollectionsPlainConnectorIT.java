package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesFrozenCollectionsPlainConnectorIT
    extends ScyllaTypesFrozenCollectionsBase<String, String> {
  /** {@inheritDoc} */
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithValues(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_list_col": {"value": [1, 2, 3]},
            "frozen_set_col": {"value": ["a", "b", "c"]},
            "frozen_map_col": {"value": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]},
            "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
            "frozen_map_ascii_col": {"value": [{"key": "ascii_key", "value": "ascii_value"}]},
            "frozen_map_bigint_col": {"value": [{"key": 1234567890123, "value": "bigint_value"}]},
            "frozen_map_boolean_col": {"value": [{"key": true, "value": "boolean_value"}]},
            "frozen_map_date_col": {"value": [{"key": 19884, "value": "date_value"}]},
            "frozen_map_decimal_col": {"value": [{"key": "12345.67", "value": "decimal_value"}]},
            "frozen_map_double_col": {"value": [{"key": 3.14159, "value": "double_value"}]},
            "frozen_map_float_col": {"value": [{"key": 2.71828, "value": "float_value"}]},
            "frozen_map_int_col": {"value": [{"key": 42, "value": "int_value"}]},
            "frozen_map_inet_col": {"value": [{"key": "127.0.0.1", "value": "inet_value"}]},
            "frozen_map_smallint_col": {"value": [{"key": 7, "value": "smallint_value"}]},
            "frozen_map_text_col": {"value": [{"key": "text_key", "value": "text_value"}]},
            "frozen_map_time_col": {"value": [{"key": 45296789000000, "value": "time_value"}]},
            "frozen_map_timestamp_col": {"value": [{"key": 1718022896789, "value": "timestamp_value"}]},
            "frozen_map_timeuuid_col": {"value": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}]},
            "frozen_map_tinyint_col": {"value": [{"key": 5, "value": "tinyint_value"}]},
            "frozen_map_uuid_col": {"value": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}]},
            "frozen_map_varchar_col": {"value": [{"key": "varchar_key", "value": "varchar_value"}]},
            "frozen_map_varint_col": {"value": [{"key": "999999999", "value": "varint_value"}]},
            "frozen_map_tuple_key_col": {"value": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}]},
            "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]}
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
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithEmpty(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_list_col": {"value": []},
            "frozen_set_col": {"value": []},
            "frozen_map_col": {"value": []},
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}},
            "frozen_map_ascii_col": {"value": []},
            "frozen_map_bigint_col": {"value": []},
            "frozen_map_boolean_col": {"value": []},
            "frozen_map_date_col": {"value": []},
            "frozen_map_decimal_col": {"value": []},
            "frozen_map_double_col": {"value": []},
            "frozen_map_float_col": {"value": []},
            "frozen_map_int_col": {"value": []},
            "frozen_map_inet_col": {"value": []},
            "frozen_map_smallint_col": {"value": []},
            "frozen_map_text_col": {"value": []},
            "frozen_map_time_col": {"value": []},
            "frozen_map_timestamp_col": {"value": []},
            "frozen_map_timeuuid_col": {"value": []},
            "frozen_map_tinyint_col": {"value": []},
            "frozen_map_uuid_col": {"value": []},
            "frozen_map_varchar_col": {"value": []},
            "frozen_map_varint_col": {"value": []},
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
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
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
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "d",
          """
            {
              "id": 1,
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
            """,
          "null"),
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [4, 5, 6]},
              "frozen_set_col": {"value": ["x", "y", "z"]},
              "frozen_map_col": {"value": [{"key": 3, "value": "three"}, {"key": 4, "value": "four"}]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 99, "tuple_member_1": "bar"}},
              "frozen_map_ascii_col": {"value": [{"key": "ascii_key_2", "value": "ascii_value_2"}]},
              "frozen_map_bigint_col": {"value": [{"key": 1234567890124, "value": "bigint_value_2"}]},
              "frozen_map_boolean_col": {"value": [{"key": false, "value": "boolean_value_2"}]},
              "frozen_map_date_col": {"value": [{"key": 19885, "value": "date_value_2"}]},
              "frozen_map_decimal_col": {"value": [{"key": "98765.43", "value": "decimal_value_2"}]},
              "frozen_map_double_col": {"value": [{"key": 2.71828, "value": "double_value_2"}]},
              "frozen_map_float_col": {"value": [{"key": 1.41421, "value": "float_value_2"}]},
              "frozen_map_int_col": {"value": [{"key": 43, "value": "int_value_2"}]},
              "frozen_map_inet_col": {"value": [{"key": "127.0.0.2", "value": "inet_value_2"}]},
              "frozen_map_smallint_col": {"value": [{"key": 8, "value": "smallint_value_2"}]},
              "frozen_map_text_col": {"value": [{"key": "text_key_2", "value": "text_value_2"}]},
              "frozen_map_time_col": {"value": [{"key": 3723456000000, "value": "time_value_2"}]},
              "frozen_map_timestamp_col": {"value": [{"key": 1718067723456, "value": "timestamp_value_2"}]},
              "frozen_map_timeuuid_col": {"value": [{"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value_2"}]},
              "frozen_map_tinyint_col": {"value": [{"key": 6, "value": "tinyint_value_2"}]},
              "frozen_map_uuid_col": {"value": [{"key": "453662fa-db4b-4938-9033-d8523c0a371d", "value": "uuid_value_2"}]},
              "frozen_map_varchar_col": {"value": [{"key": "varchar_key_2", "value": "varchar_value_2"}]},
              "frozen_map_varint_col": {"value": [{"key": "888888888", "value": "varint_value_2"}]},
              "frozen_map_tuple_key_col": {"value": [{"key": {"tuple_member_0": 2, "tuple_member_1": "tuple_key_2"}, "value": "tuple_value_2"}]},
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 2, "b": "udt_key_2"}, "value": "udt_value_2"}]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": []},
              "frozen_set_col": {"value": []},
              "frozen_map_col": {"value": []},
              "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}},
              "frozen_map_ascii_col": {"value": []},
              "frozen_map_bigint_col": {"value": []},
              "frozen_map_boolean_col": {"value": []},
              "frozen_map_date_col": {"value": []},
              "frozen_map_decimal_col": {"value": []},
              "frozen_map_double_col": {"value": []},
              "frozen_map_float_col": {"value": []},
              "frozen_map_int_col": {"value": []},
              "frozen_map_inet_col": {"value": []},
              "frozen_map_smallint_col": {"value": []},
              "frozen_map_text_col": {"value": []},
              "frozen_map_time_col": {"value": []},
              "frozen_map_timestamp_col": {"value": []},
              "frozen_map_timeuuid_col": {"value": []},
              "frozen_map_tinyint_col": {"value": []},
              "frozen_map_uuid_col": {"value": []},
              "frozen_map_varchar_col": {"value": []},
              "frozen_map_varint_col": {"value": []},
              "frozen_map_tuple_key_col": {"value": []},
              "frozen_map_udt_key_col": {"value": []}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToNull(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
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
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromEmptyToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
              "frozen_map_ascii_col": {"value": [{"key": "ascii_key", "value": "ascii_value"}]},
              "frozen_map_bigint_col": {"value": [{"key": 1234567890123, "value": "bigint_value"}]},
              "frozen_map_boolean_col": {"value": [{"key": true, "value": "boolean_value"}]},
              "frozen_map_date_col": {"value": [{"key": 19884, "value": "date_value"}]},
              "frozen_map_decimal_col": {"value": [{"key": "12345.67", "value": "decimal_value"}]},
              "frozen_map_double_col": {"value": [{"key": 3.14159, "value": "double_value"}]},
              "frozen_map_float_col": {"value": [{"key": 2.71828, "value": "float_value"}]},
              "frozen_map_int_col": {"value": [{"key": 42, "value": "int_value"}]},
              "frozen_map_inet_col": {"value": [{"key": "127.0.0.1", "value": "inet_value"}]},
              "frozen_map_smallint_col": {"value": [{"key": 7, "value": "smallint_value"}]},
              "frozen_map_text_col": {"value": [{"key": "text_key", "value": "text_value"}]},
              "frozen_map_time_col": {"value": [{"key": 45296789000000, "value": "time_value"}]},
              "frozen_map_timestamp_col": {"value": [{"key": 1718022896789, "value": "timestamp_value"}]},
              "frozen_map_timeuuid_col": {"value": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}]},
              "frozen_map_tinyint_col": {"value": [{"key": 5, "value": "tinyint_value"}]},
              "frozen_map_uuid_col": {"value": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}]},
              "frozen_map_varchar_col": {"value": [{"key": "varchar_key", "value": "varchar_value"}]},
              "frozen_map_varint_col": {"value": [{"key": "999999999", "value": "varint_value"}]},
              "frozen_map_tuple_key_col": {"value": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}]},
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]}
            }
            """)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromNullToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_list_col": {"value": [1, 2, 3]},
              "frozen_set_col": {"value": ["a", "b", "c"]},
              "frozen_map_col": {"value": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]},
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}},
              "frozen_map_ascii_col": {"value": [{"key": "ascii_key", "value": "ascii_value"}]},
              "frozen_map_bigint_col": {"value": [{"key": 1234567890123, "value": "bigint_value"}]},
              "frozen_map_boolean_col": {"value": [{"key": true, "value": "boolean_value"}]},
              "frozen_map_date_col": {"value": [{"key": 19884, "value": "date_value"}]},
              "frozen_map_decimal_col": {"value": [{"key": "12345.67", "value": "decimal_value"}]},
              "frozen_map_double_col": {"value": [{"key": 3.14159, "value": "double_value"}]},
              "frozen_map_float_col": {"value": [{"key": 2.71828, "value": "float_value"}]},
              "frozen_map_int_col": {"value": [{"key": 42, "value": "int_value"}]},
              "frozen_map_inet_col": {"value": [{"key": "127.0.0.1", "value": "inet_value"}]},
              "frozen_map_smallint_col": {"value": [{"key": 7, "value": "smallint_value"}]},
              "frozen_map_text_col": {"value": [{"key": "text_key", "value": "text_value"}]},
              "frozen_map_time_col": {"value": [{"key": 45296789000000, "value": "time_value"}]},
              "frozen_map_timestamp_col": {"value": [{"key": 1718022896789, "value": "timestamp_value"}]},
              "frozen_map_timeuuid_col": {"value": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "timeuuid_value"}]},
              "frozen_map_tinyint_col": {"value": [{"key": 5, "value": "tinyint_value"}]},
              "frozen_map_uuid_col": {"value": [{"key": "453662fa-db4b-4938-9033-d8523c0a371c", "value": "uuid_value"}]},
              "frozen_map_varchar_col": {"value": [{"key": "varchar_key", "value": "varchar_value"}]},
              "frozen_map_varint_col": {"value": [{"key": "999999999", "value": "varint_value"}]},
              "frozen_map_tuple_key_col": {"value": [{"key": {"tuple_member_0": 1, "tuple_member_1": "tuple_key"}, "value": "tuple_value"}]},
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]}
            }
            """)
    };
  }
}
