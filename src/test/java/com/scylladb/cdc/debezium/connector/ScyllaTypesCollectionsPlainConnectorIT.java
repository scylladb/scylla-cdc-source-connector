package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesCollectionsPlainConnectorIT
    extends ScyllaTypesCollectionsBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

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
            "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]},
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
            "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
            "list_col": {"value": [{"value": 10}, {"value": 20}, {"value": 30}]},
            "set_col": {"value": ["x", "y", "z"]},
            "map_col": {"value": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]},
            "udt": {"value": {"a": 7, "b": "bar"}},
            "nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
            "udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
            "udt_with_list": {"value": {"l": [1, 2, 3]}},
            "udt_with_set": {"value": {"s": ["a", "b", "c"]}}
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
            "frozen_map_udt_key_col": {"value": []},
            "frozen_udt": {"value": {"a": null, "b": null}},
            "frozen_nested_udt": {"value": {"inner": null, "z": null}},
            "frozen_udt_with_map": {"value": {"m": []}},
            "frozen_udt_with_list": {"value": {"l": []}},
            "frozen_udt_with_set": {"value": {"s": []}},
            "list_col": {"value": null},
            "set_col": {"value": null},
            "map_col": {"value": null},
            "udt": null,
            "nested_udt": null,
            "udt_with_map": {"value": {"m": []}},
            "udt_with_list": {"value": {"l": []}},
            "udt_with_set": {"value": {"s": []}}
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
            "frozen_map_udt_key_col": {"value": null},
            "frozen_udt": {"value": null},
            "frozen_nested_udt": {"value": null},
            "frozen_udt_with_map": {"value": null},
            "frozen_udt_with_list": {"value": null},
            "frozen_udt_with_set": {"value": null},
            "list_col": {"value": null},
            "set_col": {"value": null},
            "map_col": {"value": null},
            "udt": null,
            "nested_udt": null,
            "udt_with_map": {"value": {"m": []}},
            "udt_with_list": {"value": {"l": []}},
            "udt_with_set": {"value": {"s": []}}
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
              "frozen_map_udt_key_col": null,
              "frozen_udt": null,
              "frozen_nested_udt": null,
              "frozen_udt_with_map": null,
              "frozen_udt_with_list": null,
              "frozen_udt_with_set": null,
              "list_col": null,
              "set_col": null,
              "map_col": null,
              "udt": null,
              "nested_udt": null,
              "udt_with_map": null,
              "udt_with_list": null,
              "udt_with_set": null
            }
            """,
          "null"),
      null
    };
  }

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
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 2, "b": "udt_key_2"}, "value": "udt_value_2"}]},
              "frozen_udt": {"value": {"a": 99, "b": "updated"}},
              "frozen_nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "frozen_udt_with_list": {"value": {"l": [4, 5, 6]}},
              "frozen_udt_with_set": {"value": {"s": ["d", "e"]}},
              "list_col": {"value": [{"value": 40}, {"value": 50}, {"value": 60}]},
              "set_col": {"value": ["p", "q", "r"]},
              "map_col": {"value": [{"key": 30, "value": "thirty"}, {"key": 40, "value": "forty"}]},
              "udt": {"value": {"a": 100, "b": "updated"}},
              "nested_udt": {"value": {"inner": {"x": 11, "y": "updated"}, "z": 21}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "udt_with_list": {"value": {"l": [4, 5, 6]}},
              "udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

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
              "frozen_map_udt_key_col": {"value": []},
              "frozen_udt": {"value": {"a": null, "b": null}},
              "frozen_nested_udt": {"value": {"inner": null, "z": null}},
              "frozen_udt_with_map": {"value": {"m": []}},
              "frozen_udt_with_list": {"value": {"l": []}},
              "frozen_udt_with_set": {"value": {"s": []}},
              "list_col": {"value": null},
              "set_col": {"value": null},
              "map_col": {"value": null},
              "udt": {"value": null},
              "nested_udt": {"value": null},
              "udt_with_map": {"value": {"m": []}},
              "udt_with_list": {"value": {"l": []}},
              "udt_with_set": {"value": {"s": []}}
            }
            """)
    };
  }

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
              "frozen_map_udt_key_col": {"value": null},
              "frozen_udt": {"value": null},
              "frozen_nested_udt": {"value": null},
              "frozen_udt_with_map": {"value": null},
              "frozen_udt_with_list": {"value": null},
              "frozen_udt_with_set": {"value": null},
              "list_col": {"value": null},
              "set_col": {"value": null},
              "map_col": {"value": null},
              "udt": {"value": null},
              "nested_udt": {"value": null},
              "udt_with_map": {"value": {"m": []}},
              "udt_with_list": {"value": {"l": []}},
              "udt_with_set": {"value": {"s": []}}
            }
            """)
    };
  }

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
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]},
              "frozen_udt": {"value": {"a": 42, "b": "foo"}},
              "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
              "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
              "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
              "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
              "list_col": {"value": [{"value": 10}, {"value": 20}, {"value": 30}]},
              "set_col": {"value": ["x", "y", "z"]},
              "map_col": {"value": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]},
              "udt": {"value": {"a": 7, "b": "bar"}},
              "nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
              "udt_with_list": {"value": {"l": [1, 2, 3]}},
              "udt_with_set": {"value": {"s": ["a", "b", "c"]}}
            }
            """)
    };
  }

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
              "frozen_map_udt_key_col": {"value": [{"key": {"a": 1, "b": "udt_key"}, "value": "udt_value"}]},
              "frozen_udt": {"value": {"a": 42, "b": "foo"}},
              "frozen_nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
              "frozen_udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
              "frozen_udt_with_list": {"value": {"l": [1, 2, 3]}},
              "frozen_udt_with_set": {"value": {"s": ["a", "b", "c"]}},
              "list_col": {"value": [{"value": 10}, {"value": 20}, {"value": 30}]},
              "set_col": {"value": ["x", "y", "z"]},
              "map_col": {"value": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]},
              "udt": {"value": {"a": 7, "b": "bar"}},
              "nested_udt": {"value": {"inner": {"x": 10, "y": "hello"}, "z": 20}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 100}, {"key": "key2", "value": 200}]}},
              "udt_with_list": {"value": {"l": [1, 2, 3]}},
              "udt_with_set": {"value": {"s": ["a", "b", "c"]}}
            }
            """)
    };
  }

  @Override
  String[] expectedNonFrozenAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": 40}]},
              "set_col": {"value": ["w"]},
              "map_col": {"value": [{"key": 30, "value": "thirty"}]},
              "udt": {"value": {"a": 100, "b": null}},
              "nested_udt": {"value": {"inner": null, "z": 21}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "udt_with_list": {"value": {"l": [4, 5, 6]}},
              "udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  @Override
  String[] expectedNonFrozenRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": null}]},
              "set_col": {"value": ["y"]},
              "map_col": {"value": [{"key": 10, "value": null}]},
              "udt": {"value": {"a": 100, "b": null}},
              "nested_udt": {"value": {"inner": null, "z": 21}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "udt_with_list": {"value": {"l": [4, 5, 6]}},
              "udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  @Override
  String[] expectedNonFrozenAddElementFromNull(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": 40}]},
              "set_col": {"value": ["w"]},
              "map_col": {"value": [{"key": 30, "value": "thirty"}]},
              "udt": {"value": {"a": 100, "b": null}},
              "nested_udt": {"value": {"inner": null, "z": 21}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "udt_with_list": {"value": {"l": [4, 5, 6]}},
              "udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  @Override
  String[] expectedNonFrozenAddElementFromEmpty(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": 40}]},
              "set_col": {"value": ["w"]},
              "map_col": {"value": [{"key": 30, "value": "thirty"}]},
              "udt": {"value": {"a": 100, "b": null}},
              "nested_udt": {"value": {"inner": null, "z": 21}},
              "udt_with_map": {"value": {"m": [{"key": "key1", "value": 101}, {"key": "key3", "value": 300}]}},
              "udt_with_list": {"value": {"l": [4, 5, 6]}},
              "udt_with_set": {"value": {"s": ["d", "e"]}}
            }
            """)
    };
  }

  @Override
  String[] expectedNonFrozenRemoveAllElements(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": [{"value": null}, {"value": null}, {"value": null}]},
              "set_col": {"value": ["x", "y", "z"]},
              "map_col": {"value": [{"key": 10, "value": null}, {"key": 20, "value": null}]},
              "udt": {"value": {"a": null, "b": null}},
              "nested_udt": {"value": {"inner": null, "z": null}},
              "udt_with_map": {"value": {"m": []}},
              "udt_with_list": {"value": {"l": []}},
              "udt_with_set": {"value": {"s": []}}
            }
            """)
    };
  }
}
