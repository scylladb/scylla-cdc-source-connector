package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesPrimitiveAvroConnectorIT
    extends ScyllaTypesPrimitiveBase<GenericRecord, GenericRecord> {

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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record with full postimage
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // DELETE record: This table has only partition key (no clustering key), so DELETE
      // becomes PARTITION_DELETE. Scylla doesn't send preimage for PARTITION_DELETE,
      // so "before" only has the primary key column.
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
  String[] expectedUpdateFromValueToNil(int pk) {
    return new String[] {
      // INSERT record with full postimage
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record with preimage and postimage
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null (row didn't exist), after has full postimage
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has preimage, after has postimage
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromValueToValue(int pk) {
    return new String[] {
      // INSERT record: before is null (row didn't exist), after has full postimage
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has preimage, after has postimage
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToValue(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns (other cols were nil)
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has only untouched_* columns, after has all values
      """
        {
          "before": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has only untouched_* columns, after has new values
      """
        {
          "before": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromNilToNil(int pk) {
    return new String[] {
      // INSERT record: before is null, after has only untouched_* columns
      """
        {
          "before": null,
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has only untouched_* columns, after also has only untouched_* columns
      """
        {
          "before": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToValue(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has values with empty strings, after has new values
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToNil(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has values with empty strings, after has only untouched_* columns
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
            "id": %d,
            "untouched_text": "%s",
            "untouched_int": %d,
            "untouched_boolean": %s,
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }

  @Override
  String[] expectedUpdateFromEmptyToEmpty(int pk) {
    return new String[] {
      // INSERT record: before is null, after has values with empty strings
      """
        {
          "before": null,
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              expectedSource()),
      // UPDATE record: before has values with empty strings, after has new values
      """
        {
          "before": {
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
            "untouched_uuid": "%s"
          },
          "after": {
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
            "untouched_uuid": "%s"
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
              pk,
              UNTOUCHED_TEXT_VALUE,
              UNTOUCHED_INT_VALUE,
              UNTOUCHED_BOOLEAN_VALUE,
              UNTOUCHED_UUID_VALUE,
              pk,
              expectedSource())
    };
  }
}
