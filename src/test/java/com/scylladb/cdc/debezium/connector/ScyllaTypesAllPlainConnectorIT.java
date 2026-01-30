package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;
import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Plain connector integration tests for all Scylla types. Provides expected JSON for all test
 * scenarios.
 */
public class ScyllaTypesAllPlainConnectorIT extends ScyllaTypesAllBase<String, String> {

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  protected int extractPkFromValue(String value) {
    int pk = extractIdFromKeyField(value);
    if (pk != -1) {
      return pk;
    }
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Override
  protected String blobValueSet1() {
    return "yv66vg=="; // base64 for 0xCAFEBABE
  }

  @Override
  protected String blobValueSet2() {
    return "3q2+7w=="; // base64 for 0xDEADBEEF
  }

  // ===================== Helper: Expected Key =====================

  private String expectedKeyFor(int pk) {
    return "{\"id\": %d}".formatted(pk);
  }

  // ===================== Helper: All Columns Null =====================

  private static final String ALL_COLS_NULL =
      """
      "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
      "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
      "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
      "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
      "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
      "frozen_list_col": null, "frozen_set_col": null, "frozen_map_col": null,
      "frozen_tuple_col": null, "frozen_map_ascii_col": null, "frozen_map_bigint_col": null,
      "frozen_map_boolean_col": null, "frozen_map_date_col": null, "frozen_map_decimal_col": null,
      "frozen_map_double_col": null, "frozen_map_float_col": null, "frozen_map_int_col": null,
      "frozen_map_inet_col": null, "frozen_map_smallint_col": null, "frozen_map_text_col": null,
      "frozen_map_time_col": null, "frozen_map_timestamp_col": null, "frozen_map_timeuuid_col": null,
      "frozen_map_tinyint_col": null, "frozen_map_uuid_col": null, "frozen_map_varchar_col": null,
      "frozen_map_varint_col": null, "frozen_map_tuple_key_col": null, "frozen_map_udt_key_col": null,
      "frozen_set_timeuuid_col": null, "list_col": null, "set_col": null, "map_col": null,
      "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
      "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
      "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
      "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
      "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null,
      "frozen_udt": null, "nf_udt": null, "frozen_nested_udt": null, "nf_nested_udt": null,
      "frozen_udt_with_map": null, "nf_udt_with_map": null, "frozen_udt_with_list": null,
      "nf_udt_with_list": null, "frozen_udt_with_set": null, "nf_udt_with_set": null
      """;

  // ===================== Primitive Expected Methods =====================

  @Override
  String[] expectedPrimitiveInsert(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "some text",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              nonPrimitiveColsNull(),
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedPrimitiveDelete(int pk) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "delete me",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
          .formatted(
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              nonPrimitiveColsNull(),
              pk,
              expectedSource()),
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
      primitiveInsertRecord(pk, "value"), primitiveUpdateRecord(pk, "value", true)
    };
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToEmpty(int pk) {
    return new String[] {primitiveInsertRecord(pk, "value"), primitiveUpdateToEmptyRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromValueToValue(int pk) {
    return new String[] {primitiveInsertRecord(pk, "value"), primitiveUpdateToValueRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToValue(int pk) {
    return new String[] {primitiveInsertNullRecord(pk), primitiveNilToValueRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToEmpty(int pk) {
    return new String[] {primitiveInsertNullRecord(pk), primitiveNilToEmptyRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromNilToNil(int pk) {
    return new String[] {primitiveInsertNullRecord(pk), primitiveNilToNilRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToValue(int pk) {
    return new String[] {primitiveInsertEmptyRecord(pk), primitiveEmptyToValueRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToNil(int pk) {
    return new String[] {primitiveInsertEmptyRecord(pk), primitiveEmptyToNilRecord(pk)};
  }

  @Override
  String[] expectedPrimitiveUpdateFromEmptyToEmpty(int pk) {
    return new String[] {primitiveInsertEmptyRecord(pk), primitiveEmptyToEmptyRecord(pk)};
  }

  // ===================== Frozen Collections Expected Methods =====================

  @Override
  String[] expectedFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithEmpty(int pk) {
    return new String[] {expectedRecord("c", "null", frozenAfterEmpty(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {expectedRecord("c", "null", frozenAfterNull(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedFrozenCollectionsDelete(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord("d", "null", "null", expectedKeyFor(pk)),
      null
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord(
          "u", frozenAfterWithValues(pk), frozenAfterUpdatedValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord("u", frozenAfterWithValues(pk), frozenAfterEmpty(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord("u", frozenAfterWithValues(pk), frozenAfterAllNull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterEmpty(pk), expectedKeyFor(pk)),
      expectedRecord("u", frozenAfterEmpty(pk), frozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", frozenAfterAllNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", frozenAfterAllNull(pk), frozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  // ===================== Non-Frozen Collections Expected Methods =====================

  @Override
  String[] expectedNonFrozenCollectionsInsertWithValues(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsInsertWithNull(int pk) {
    return new String[] {expectedRecord("c", "null", nonFrozenAfterNull(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedNonFrozenCollectionsDelete(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterMinimal(pk), expectedKeyFor(pk)),
      expectedRecord("d", "null", "null", expectedKeyFor(pk)),
      null
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListAddElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterListOnly(pk, "[10, 20]"), expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterListOnly(pk, "[10, 20]"),
          nonFrozenAfterListOnly(pk, "[10, 20, 30]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetAddElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterSetOnly(pk, "[\"x\", \"y\"]"), expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterSetOnly(pk, "[\"x\", \"y\"]"),
          nonFrozenAfterSetOnly(pk, "[\"x\", \"y\", \"z\"]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapAddElement(int pk) {
    return new String[] {
      expectedRecord(
          "c",
          "null",
          nonFrozenAfterMapOnly(pk, "[{\"key\": 10, \"value\": \"ten\"}]"),
          expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterMapOnly(pk, "[{\"key\": 10, \"value\": \"ten\"}]"),
          nonFrozenAfterMapOnly(
              pk, "[{\"key\": 10, \"value\": \"ten\"}, {\"key\": 20, \"value\": \"twenty\"}]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateListRemoveElement(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterListOnly(pk, "[10, 20, 30]"), expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterListOnly(pk, "[10, 20, 30]"),
          nonFrozenAfterListOnly(pk, "[10, 30]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateSetRemoveElement(int pk) {
    return new String[] {
      expectedRecord(
          "c", "null", nonFrozenAfterSetOnly(pk, "[\"x\", \"y\", \"z\"]"), expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterSetOnly(pk, "[\"x\", \"y\", \"z\"]"),
          nonFrozenAfterSetOnly(pk, "[\"x\", \"z\"]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateMapRemoveElement(int pk) {
    return new String[] {
      expectedRecord(
          "c",
          "null",
          nonFrozenAfterMapOnly(
              pk, "[{\"key\": 10, \"value\": \"ten\"}, {\"key\": 20, \"value\": \"twenty\"}]"),
          expectedKeyFor(pk)),
      expectedRecord(
          "u",
          nonFrozenAfterMapOnly(
              pk, "[{\"key\": 10, \"value\": \"ten\"}, {\"key\": 20, \"value\": \"twenty\"}]"),
          nonFrozenAfterMapOnly(pk, "[{\"key\": 20, \"value\": \"twenty\"}]"),
          expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord(
          "u", nonFrozenAfterWithValues(pk), nonFrozenAfterUpdatedValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord("u", nonFrozenAfterWithValues(pk), nonFrozenAfterNull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromValueToEmpty(int pk) {
    // Empty non-frozen collections become null in Scylla
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterWithValues(pk), expectedKeyFor(pk)),
      expectedRecord("u", nonFrozenAfterWithValues(pk), nonFrozenAfterNull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromNullToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", nonFrozenAfterNull(pk), nonFrozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedNonFrozenCollectionsUpdateFromEmptyToValue(int pk) {
    // Empty non-frozen collections are stored as null in Scylla
    return new String[] {
      expectedRecord("c", "null", nonFrozenAfterNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", nonFrozenAfterNull(pk), nonFrozenAfterWithValues(pk), expectedKeyFor(pk))
    };
  }

  // ===================== UDT Expected Methods =====================

  @Override
  String[] expectedUdtInsert(int pk) {
    return new String[] {expectedRecord("c", "null", udtAfterFull(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedUdtInsertWithNull(int pk) {
    return new String[] {expectedRecord("c", "null", udtAfterNull(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedUdtInsertWithEmpty(int pk) {
    // Empty UDTs (all fields null) are represented as null in Scylla
    return new String[] {expectedRecord("c", "null", udtAfterNull(pk), expectedKeyFor(pk))};
  }

  @Override
  String[] expectedUdtDelete(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterFull(pk), expectedKeyFor(pk)),
      expectedRecord("d", "null", "null", expectedKeyFor(pk)),
      null
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterForFrozenUpdate(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterForFrozenUpdate(pk), udtAfterUpdated(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterForValueToNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterForValueToNull(pk), udtAfterNull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromValueToEmpty(int pk) {
    // Empty UDTs are represented as null in Scylla
    return new String[] {
      expectedRecord("c", "null", udtAfterFull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterFull(pk), udtAfterNull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromNullToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterNull(pk), udtAfterFull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateFrozenUdtFromEmptyToValue(int pk) {
    // Empty UDTs are represented as null in Scylla
    return new String[] {
      expectedRecord("c", "null", udtAfterNull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterNull(pk), udtAfterFull(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtField(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterForNonFrozenField(pk), expectedKeyFor(pk)),
      expectedRecord(
          "u", udtAfterForNonFrozenField(pk), udtAfterNonFrozenFieldUpdated(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToValue(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterFull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterFull(pk), udtAfterNonFrozenReplaced(pk), expectedKeyFor(pk))
    };
  }

  @Override
  String[] expectedUdtUpdateNonFrozenUdtFromValueToNull(int pk) {
    return new String[] {
      expectedRecord("c", "null", udtAfterFull(pk), expectedKeyFor(pk)),
      expectedRecord("u", udtAfterFull(pk), udtAfterNonFrozenNull(pk), expectedKeyFor(pk))
    };
  }

  // ===================== Helper Methods for Primitive JSON =====================

  private String nonPrimitiveColsNull() {
    return """
        "frozen_list_col": null, "frozen_set_col": null, "frozen_map_col": null,
        "frozen_tuple_col": null, "frozen_map_ascii_col": null, "frozen_map_bigint_col": null,
        "frozen_map_boolean_col": null, "frozen_map_date_col": null, "frozen_map_decimal_col": null,
        "frozen_map_double_col": null, "frozen_map_float_col": null, "frozen_map_int_col": null,
        "frozen_map_inet_col": null, "frozen_map_smallint_col": null, "frozen_map_text_col": null,
        "frozen_map_time_col": null, "frozen_map_timestamp_col": null, "frozen_map_timeuuid_col": null,
        "frozen_map_tinyint_col": null, "frozen_map_uuid_col": null, "frozen_map_varchar_col": null,
        "frozen_map_varint_col": null, "frozen_map_tuple_key_col": null, "frozen_map_udt_key_col": null,
        "frozen_set_timeuuid_col": null, "list_col": null, "set_col": null, "map_col": null,
        "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
        "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
        "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
        "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
        "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null,
        "frozen_udt": null, "nf_udt": null, "frozen_nested_udt": null, "nf_nested_udt": null,
        "frozen_udt_with_map": null, "nf_udt_with_map": null, "frozen_udt_with_list": null,
        "nf_udt_with_list": null, "frozen_udt_with_set": null, "nf_udt_with_set": null
        """;
  }

  private String primitiveInsertRecord(int pk, String textCol) {
    return """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
        .formatted(
            pk,
            textCol,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveUpdateRecord(int pk, String textColBefore, boolean afterIsNull) {
    String afterValues =
        afterIsNull
            ? """
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null
            """
            : """
            "ascii_col": "ascii", "bigint_col": 1234567890123, "blob_col": "yv66vg==",
            "boolean_col": true, "date_col": 19884, "decimal_col": "12345.67",
            "double_col": 3.14159, "duration_col": "1d12h30m", "float_col": 2.71828,
            "inet_col": "127.0.0.1", "int_col": 42, "smallint_col": 7, "text_col": "value",
            "time_col": 45296789000000, "timestamp_col": 1718022896789,
            "timeuuid_col": "81d4a030-4632-11f0-9484-409dd8f36eba", "tinyint_col": 5,
            "uuid_col": "453662fa-db4b-4938-9033-d8523c0a371c", "varchar_col": "varchar text",
            "varint_col": "999999999"
            """;

    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            %s,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            textColBefore,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            afterValues,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveUpdateToEmptyRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveUpdateToValueRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii2",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "value2",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveInsertNullRecord(int pk) {
    return """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveNilToValueRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
            "boolean_col": true,
            "date_col": 19884,
            "decimal_col": "12345.67",
            "double_col": 3.14159,
            "duration_col": "1d12h30m",
            "float_col": 2.71828,
            "inet_col": "127.0.0.1",
            "int_col": 42,
            "smallint_col": 7,
            "text_col": "value",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveNilToEmptyRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveNilToNilRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveInsertEmptyRecord(int pk) {
    return """
        {
          "before": null,
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "c",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveEmptyToValueRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "ascii2",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
            "boolean_col": false,
            "date_col": 19885,
            "decimal_col": "98765.43",
            "double_col": 2.71828,
            "duration_col": "2d1h",
            "float_col": 1.41421,
            "inet_col": "127.0.0.2",
            "int_col": 43,
            "smallint_col": 8,
            "text_col": "value2",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveEmptyToNilRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
            "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
            "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
            "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
            "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  private String primitiveEmptyToEmptyRecord(int pk) {
    return """
        {
          "before": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890123,
            "blob_col": "yv66vg==",
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
            "untouched_uuid": "%s",
            %s
          },
          "after": {
            "id": %d,
            "ascii_col": "",
            "bigint_col": 1234567890124,
            "blob_col": "3q2+7w==",
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
            "untouched_uuid": "%s",
            %s
          },
          "key": {"id": %d},
          "op": "u",
          "source": %s
        }
        """
        .formatted(
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            UNTOUCHED_TEXT_VALUE,
            UNTOUCHED_INT_VALUE,
            UNTOUCHED_BOOLEAN_VALUE,
            UNTOUCHED_UUID_VALUE,
            nonPrimitiveColsNull(),
            pk,
            expectedSource());
  }

  // ===================== Helper Methods for Frozen Collections JSON =====================

  private String otherColsNull() {
    return """
        "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
        "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
        "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
        "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
        "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
        "untouched_text": null, "untouched_int": null, "untouched_boolean": null, "untouched_uuid": null,
        "list_col": null, "set_col": null, "map_col": null,
        "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
        "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
        "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
        "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
        "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null,
        "frozen_udt": null, "nf_udt": null, "frozen_nested_udt": null, "nf_nested_udt": null,
        "frozen_udt_with_map": null, "nf_udt_with_map": null, "frozen_udt_with_list": null,
        "nf_udt_with_list": null, "frozen_udt_with_set": null, "nf_udt_with_set": null
        """;
  }

  private String frozenAfterWithValues(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        .formatted(pk, otherColsNull());
  }

  private String frozenAfterUpdatedValues(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        .formatted(pk, otherColsNull());
  }

  private String frozenAfterEmpty(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        .formatted(pk, otherColsNull());
  }

  private String frozenAfterNull(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        .formatted(pk, otherColsNull());
  }

  private String frozenAfterAllNull(int pk) {
    return frozenAfterNull(pk);
  }

  // ===================== Helper Methods for Non-Frozen Collections JSON =====================

  private String nonFrozenColsNull() {
    return """
        "list_col": null, "set_col": null, "map_col": null,
        "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
        "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
        "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
        "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
        "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        """;
  }

  private String otherColsNullForNonFrozen() {
    return """
        "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
        "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
        "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
        "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
        "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
        "untouched_text": null, "untouched_int": null, "untouched_boolean": null, "untouched_uuid": null,
        "frozen_list_col": null, "frozen_set_col": null, "frozen_map_col": null,
        "frozen_tuple_col": null, "frozen_map_ascii_col": null, "frozen_map_bigint_col": null,
        "frozen_map_boolean_col": null, "frozen_map_date_col": null, "frozen_map_decimal_col": null,
        "frozen_map_double_col": null, "frozen_map_float_col": null, "frozen_map_int_col": null,
        "frozen_map_inet_col": null, "frozen_map_smallint_col": null, "frozen_map_text_col": null,
        "frozen_map_time_col": null, "frozen_map_timestamp_col": null, "frozen_map_timeuuid_col": null,
        "frozen_map_tinyint_col": null, "frozen_map_uuid_col": null, "frozen_map_varchar_col": null,
        "frozen_map_varint_col": null, "frozen_map_tuple_key_col": null, "frozen_map_udt_key_col": null,
        "frozen_set_timeuuid_col": null,
        "frozen_udt": null, "nf_udt": null, "frozen_nested_udt": null, "nf_nested_udt": null,
        "frozen_udt_with_map": null, "nf_udt_with_map": null, "frozen_udt_with_list": null,
        "nf_udt_with_list": null, "frozen_udt_with_set": null, "nf_udt_with_set": null
        """;
  }

  private String nonFrozenAfterWithValues(int pk) {
    return """
        {
          "id": %d,
          %s,
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
          "set_timeuuid_col": ["81d4a030-4632-11f0-9484-409dd8f36eba"]
        }
        """
        .formatted(pk, otherColsNullForNonFrozen());
  }

  private String nonFrozenAfterUpdatedValues(int pk) {
    return """
        {
          "id": %d,
          %s,
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
          "set_timeuuid_col": ["81d4a031-4632-11f0-9484-409dd8f36eba"]
        }
        """
        .formatted(pk, otherColsNullForNonFrozen());
  }

  private String nonFrozenAfterNull(int pk) {
    return """
        {
          "id": %d,
          %s,
          %s
        }
        """
        .formatted(pk, otherColsNullForNonFrozen(), nonFrozenColsNull());
  }

  private String nonFrozenAfterMinimal(int pk) {
    return """
        {
          "id": %d,
          %s,
          "list_col": [10],
          "set_col": ["x"],
          "map_col": [{"key": 10, "value": "ten"}],
          "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
          "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
          "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
          "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
          "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        }
        """
        .formatted(pk, otherColsNullForNonFrozen());
  }

  private String nonFrozenAfterListOnly(int pk, String listValue) {
    return """
        {
          "id": %d,
          %s,
          "list_col": %s,
          "set_col": null, "map_col": null,
          "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
          "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
          "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
          "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
          "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        }
        """
        .formatted(pk, otherColsNullForNonFrozen(), listValue);
  }

  private String nonFrozenAfterSetOnly(int pk, String setValue) {
    return """
        {
          "id": %d,
          %s,
          "list_col": null,
          "set_col": %s,
          "map_col": null,
          "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
          "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
          "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
          "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
          "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        }
        """
        .formatted(pk, otherColsNullForNonFrozen(), setValue);
  }

  private String nonFrozenAfterMapOnly(int pk, String mapValue) {
    return """
        {
          "id": %d,
          %s,
          "list_col": null,
          "set_col": null,
          "map_col": %s,
          "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
          "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
          "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
          "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
          "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        }
        """
        .formatted(pk, otherColsNullForNonFrozen(), mapValue);
  }

  // ===================== Helper Methods for UDT JSON =====================

  private String otherColsNullForUdt() {
    return """
        "ascii_col": null, "bigint_col": null, "blob_col": null, "boolean_col": null,
        "date_col": null, "decimal_col": null, "double_col": null, "duration_col": null,
        "float_col": null, "inet_col": null, "int_col": null, "smallint_col": null,
        "text_col": null, "time_col": null, "timestamp_col": null, "timeuuid_col": null,
        "tinyint_col": null, "uuid_col": null, "varchar_col": null, "varint_col": null,
        "untouched_text": null, "untouched_int": null, "untouched_boolean": null, "untouched_uuid": null,
        "frozen_list_col": null, "frozen_set_col": null, "frozen_map_col": null,
        "frozen_tuple_col": null, "frozen_map_ascii_col": null, "frozen_map_bigint_col": null,
        "frozen_map_boolean_col": null, "frozen_map_date_col": null, "frozen_map_decimal_col": null,
        "frozen_map_double_col": null, "frozen_map_float_col": null, "frozen_map_int_col": null,
        "frozen_map_inet_col": null, "frozen_map_smallint_col": null, "frozen_map_text_col": null,
        "frozen_map_time_col": null, "frozen_map_timestamp_col": null, "frozen_map_timeuuid_col": null,
        "frozen_map_tinyint_col": null, "frozen_map_uuid_col": null, "frozen_map_varchar_col": null,
        "frozen_map_varint_col": null, "frozen_map_tuple_key_col": null, "frozen_map_udt_key_col": null,
        "frozen_set_timeuuid_col": null,
        "list_col": null, "set_col": null, "map_col": null,
        "map_ascii_col": null, "map_bigint_col": null, "map_boolean_col": null, "map_date_col": null,
        "map_decimal_col": null, "map_double_col": null, "map_float_col": null, "map_int_col": null,
        "map_inet_col": null, "map_smallint_col": null, "map_text_col": null, "map_time_col": null,
        "map_timestamp_col": null, "map_timeuuid_col": null, "map_tinyint_col": null, "map_uuid_col": null,
        "map_varchar_col": null, "map_varint_col": null, "set_timeuuid_col": null
        """;
  }

  private String udtAfterFull(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterNull(int pk) {
    return """
        {
          "id": %d,
          %s,
          "frozen_udt": null, "nf_udt": null, "frozen_nested_udt": null, "nf_nested_udt": null,
          "frozen_udt_with_map": null, "nf_udt_with_map": null, "frozen_udt_with_list": null,
          "nf_udt_with_list": null, "frozen_udt_with_set": null, "nf_udt_with_set": null
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterForFrozenUpdate(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterUpdated(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterForValueToNull(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterForNonFrozenField(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterNonFrozenFieldUpdated(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterNonFrozenReplaced(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }

  private String udtAfterNonFrozenNull(int pk) {
    return """
        {
          "id": %d,
          %s,
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
        }
        """
        .formatted(pk, otherColsNullForUdt());
  }
}
