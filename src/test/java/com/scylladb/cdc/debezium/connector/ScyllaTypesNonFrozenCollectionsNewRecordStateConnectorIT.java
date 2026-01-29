package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesNonFrozenCollectionsNewRecordStateConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<String, String> {

  // Expected map values for insert/update tests with all columns populated
  private static final String MAP_ASCII_COL_VALUE =
      "\"map_ascii_col\": [{\"key\": \"ascii_key\", \"value\": \"ascii_value\"}]";
  private static final String MAP_BIGINT_COL_VALUE =
      "\"map_bigint_col\": [{\"key\": 1234567890123, \"value\": \"bigint_value\"}]";
  private static final String MAP_BOOLEAN_COL_VALUE =
      "\"map_boolean_col\": [{\"key\": true, \"value\": \"boolean_value\"}]";
  private static final String MAP_DATE_COL_VALUE =
      "\"map_date_col\": [{\"key\": 19884, \"value\": \"date_value\"}]";
  private static final String MAP_DECIMAL_COL_VALUE =
      "\"map_decimal_col\": [{\"key\": \"12345.67\", \"value\": \"decimal_value\"}]";
  private static final String MAP_DOUBLE_COL_VALUE =
      "\"map_double_col\": [{\"key\": 3.14159, \"value\": \"double_value\"}]";
  private static final String MAP_FLOAT_COL_VALUE =
      "\"map_float_col\": [{\"key\": 2.71828, \"value\": \"float_value\"}]";
  private static final String MAP_INET_COL_VALUE =
      "\"map_inet_col\": [{\"key\": \"127.0.0.1\", \"value\": \"inet_value\"}]";
  private static final String MAP_INT_COL_VALUE =
      "\"map_int_col\": [{\"key\": 42, \"value\": \"int_value\"}]";
  private static final String MAP_SMALLINT_COL_VALUE =
      "\"map_smallint_col\": [{\"key\": 7, \"value\": \"smallint_value\"}]";
  private static final String MAP_TEXT_COL_VALUE =
      "\"map_text_col\": [{\"key\": \"text_key\", \"value\": \"text_value\"}]";
  private static final String MAP_TIME_COL_VALUE =
      "\"map_time_col\": [{\"key\": 45296789000000, \"value\": \"time_value\"}]";
  private static final String MAP_TIMESTAMP_COL_VALUE =
      "\"map_timestamp_col\": [{\"key\": 1718022896789, \"value\": \"timestamp_value\"}]";
  private static final String MAP_TIMEUUID_COL_VALUE =
      "\"map_timeuuid_col\": [{\"key\": \"81d4a030-4632-11f0-9484-409dd8f36eba\", \"value\": \"timeuuid_value\"}]";
  private static final String MAP_TINYINT_COL_VALUE =
      "\"map_tinyint_col\": [{\"key\": 5, \"value\": \"tinyint_value\"}]";
  private static final String MAP_UUID_COL_VALUE =
      "\"map_uuid_col\": [{\"key\": \"453662fa-db4b-4938-9033-d8523c0a371c\", \"value\": \"uuid_value\"}]";
  private static final String MAP_VARCHAR_COL_VALUE =
      "\"map_varchar_col\": [{\"key\": \"varchar_key\", \"value\": \"varchar_value\"}]";
  private static final String MAP_VARINT_COL_VALUE =
      "\"map_varint_col\": [{\"key\": \"999999999\", \"value\": \"varint_value\"}]";
  private static final String SET_TIMEUUID_COL_VALUE =
      "\"set_timeuuid_col\": [\"81d4a030-4632-11f0-9484-409dd8f36eba\"]";

  // Expected map values for update tests (different values)
  private static final String MAP_ASCII_COL_VALUE_2 =
      "\"map_ascii_col\": [{\"key\": \"ascii_key_2\", \"value\": \"ascii_value_2\"}]";
  private static final String MAP_BIGINT_COL_VALUE_2 =
      "\"map_bigint_col\": [{\"key\": 1234567890124, \"value\": \"bigint_value_2\"}]";
  private static final String MAP_BOOLEAN_COL_VALUE_2 =
      "\"map_boolean_col\": [{\"key\": false, \"value\": \"boolean_value_2\"}]";
  private static final String MAP_DATE_COL_VALUE_2 =
      "\"map_date_col\": [{\"key\": 19885, \"value\": \"date_value_2\"}]";
  private static final String MAP_DECIMAL_COL_VALUE_2 =
      "\"map_decimal_col\": [{\"key\": \"98765.43\", \"value\": \"decimal_value_2\"}]";
  private static final String MAP_DOUBLE_COL_VALUE_2 =
      "\"map_double_col\": [{\"key\": 2.71828, \"value\": \"double_value_2\"}]";
  private static final String MAP_FLOAT_COL_VALUE_2 =
      "\"map_float_col\": [{\"key\": 1.41421, \"value\": \"float_value_2\"}]";
  private static final String MAP_INET_COL_VALUE_2 =
      "\"map_inet_col\": [{\"key\": \"127.0.0.2\", \"value\": \"inet_value_2\"}]";
  private static final String MAP_INT_COL_VALUE_2 =
      "\"map_int_col\": [{\"key\": 43, \"value\": \"int_value_2\"}]";
  private static final String MAP_SMALLINT_COL_VALUE_2 =
      "\"map_smallint_col\": [{\"key\": 8, \"value\": \"smallint_value_2\"}]";
  private static final String MAP_TEXT_COL_VALUE_2 =
      "\"map_text_col\": [{\"key\": \"text_key_2\", \"value\": \"text_value_2\"}]";
  private static final String MAP_TIME_COL_VALUE_2 =
      "\"map_time_col\": [{\"key\": 3723456000000, \"value\": \"time_value_2\"}]";
  private static final String MAP_TIMESTAMP_COL_VALUE_2 =
      "\"map_timestamp_col\": [{\"key\": 1718067723456, \"value\": \"timestamp_value_2\"}]";
  private static final String MAP_TIMEUUID_COL_VALUE_2 =
      "\"map_timeuuid_col\": [{\"key\": \"81d4a031-4632-11f0-9484-409dd8f36eba\", \"value\": \"timeuuid_value_2\"}]";
  private static final String MAP_TINYINT_COL_VALUE_2 =
      "\"map_tinyint_col\": [{\"key\": 6, \"value\": \"tinyint_value_2\"}]";
  private static final String MAP_UUID_COL_VALUE_2 =
      "\"map_uuid_col\": [{\"key\": \"453662fa-db4b-4938-9033-d8523c0a371d\", \"value\": \"uuid_value_2\"}]";
  private static final String MAP_VARCHAR_COL_VALUE_2 =
      "\"map_varchar_col\": [{\"key\": \"varchar_key_2\", \"value\": \"varchar_value_2\"}]";
  private static final String MAP_VARINT_COL_VALUE_2 =
      "\"map_varint_col\": [{\"key\": \"888888888\", \"value\": \"varint_value_2\"}]";
  private static final String SET_TIMEUUID_COL_VALUE_2 =
      "\"set_timeuuid_col\": [\"81d4a031-4632-11f0-9484-409dd8f36eba\"]";

  // Null values for all extra columns
  private static final String ALL_EXTRA_COLS_NULL =
      "\"map_ascii_col\": null, \"map_bigint_col\": null, \"map_boolean_col\": null, "
          + "\"map_date_col\": null, \"map_decimal_col\": null, \"map_double_col\": null, "
          + "\"map_float_col\": null, \"map_inet_col\": null, \"map_int_col\": null, "
          + "\"map_smallint_col\": null, \"map_text_col\": null, \"map_time_col\": null, "
          + "\"map_timestamp_col\": null, \"map_timeuuid_col\": null, \"map_tinyint_col\": null, "
          + "\"map_uuid_col\": null, \"map_varchar_col\": null, \"map_varint_col\": null, "
          + "\"set_timeuuid_col\": null";

  // All extra columns with initial values
  private static final String ALL_EXTRA_COLS_VALUES =
      MAP_ASCII_COL_VALUE
          + ", "
          + MAP_BIGINT_COL_VALUE
          + ", "
          + MAP_BOOLEAN_COL_VALUE
          + ", "
          + MAP_DATE_COL_VALUE
          + ", "
          + MAP_DECIMAL_COL_VALUE
          + ", "
          + MAP_DOUBLE_COL_VALUE
          + ", "
          + MAP_FLOAT_COL_VALUE
          + ", "
          + MAP_INET_COL_VALUE
          + ", "
          + MAP_INT_COL_VALUE
          + ", "
          + MAP_SMALLINT_COL_VALUE
          + ", "
          + MAP_TEXT_COL_VALUE
          + ", "
          + MAP_TIME_COL_VALUE
          + ", "
          + MAP_TIMESTAMP_COL_VALUE
          + ", "
          + MAP_TIMEUUID_COL_VALUE
          + ", "
          + MAP_TINYINT_COL_VALUE
          + ", "
          + MAP_UUID_COL_VALUE
          + ", "
          + MAP_VARCHAR_COL_VALUE
          + ", "
          + MAP_VARINT_COL_VALUE
          + ", "
          + SET_TIMEUUID_COL_VALUE;

  // All extra columns with updated values
  private static final String ALL_EXTRA_COLS_VALUES_2 =
      MAP_ASCII_COL_VALUE_2
          + ", "
          + MAP_BIGINT_COL_VALUE_2
          + ", "
          + MAP_BOOLEAN_COL_VALUE_2
          + ", "
          + MAP_DATE_COL_VALUE_2
          + ", "
          + MAP_DECIMAL_COL_VALUE_2
          + ", "
          + MAP_DOUBLE_COL_VALUE_2
          + ", "
          + MAP_FLOAT_COL_VALUE_2
          + ", "
          + MAP_INET_COL_VALUE_2
          + ", "
          + MAP_INT_COL_VALUE_2
          + ", "
          + MAP_SMALLINT_COL_VALUE_2
          + ", "
          + MAP_TEXT_COL_VALUE_2
          + ", "
          + MAP_TIME_COL_VALUE_2
          + ", "
          + MAP_TIMESTAMP_COL_VALUE_2
          + ", "
          + MAP_TIMEUUID_COL_VALUE_2
          + ", "
          + MAP_TINYINT_COL_VALUE_2
          + ", "
          + MAP_UUID_COL_VALUE_2
          + ", "
          + MAP_VARCHAR_COL_VALUE_2
          + ", "
          + MAP_VARINT_COL_VALUE_2
          + ", "
          + SET_TIMEUUID_COL_VALUE_2;

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
    // NewRecordState extracts the "after" field directly
    return new String[] {
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedInsertWithNull(int pk) {
    return new String[] {
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "id": %d,
          "list_col": [10],
          "set_col": ["x"],
          "map_col": [{"key": 10, "value": "ten"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // DELETE record - after is null, so the record is null
      null
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListAddElement(int pk) {
    return new String[] {
      // Preimage: initial state after insert
      """
        {
          "id": %d,
          "list_col": [10, 20],
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // Postimage: after appending [30]
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetAddElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": ["x", "y"],
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // UPDATE record with full after state
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": ["x", "y", "z"],
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapAddElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // UPDATE record with full after state
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateListRemoveElement(int pk) {
    return new String[] {
      // Preimage: initial state after insert
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // Postimage: after removing [20] - values 10 and 30 remain
      """
        {
          "id": %d,
          "list_col": [10, 30],
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateSetRemoveElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": ["x", "y", "z"],
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // UPDATE record with full after state (y removed, x and z remain)
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": ["x", "z"],
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateMapRemoveElement(int pk) {
    return new String[] {
      // INSERT record
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // UPDATE record with full after state (key 10 removed, key 20 remains)
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": [{"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      // Preimage: initial state after insert
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES),
      // Postimage: after replacing with new values
      """
        {
          "id": %d,
          "list_col": [40, 50, 60],
          "set_col": ["a", "b", "c"],
          "map_col": [{"key": 30, "value": "thirty"}, {"key": 40, "value": "forty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES_2)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToNull(int pk) {
    return new String[] {
      // Preimage: initial state after insert
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES),
      // Postimage: all collections set to null
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // Preimage: initial state after insert
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES),
      // Postimage: empty collections become null in Scylla
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromNullToValue(int pk) {
    return new String[] {
      // Preimage: initial state with null collections
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // Postimage: populated collections
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES)
    };
  }

  /** {@inheritDoc} */
  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    // Empty collections are stored as null in Scylla
    return new String[] {
      // Preimage: empty collections become null
      """
        {
          "id": %d,
          "list_col": null,
          "set_col": null,
          "map_col": null,
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_NULL),
      // Postimage: populated collections
      """
        {
          "id": %d,
          "list_col": [10, 20, 30],
          "set_col": ["x", "y", "z"],
          "map_col": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}],
          %s
        }
        """
          .formatted(pk, ALL_EXTRA_COLS_VALUES)
    };
  }
}
