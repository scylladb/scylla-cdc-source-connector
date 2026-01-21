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
  static void checkKafkaProvider() {
    Assumptions.assumeTrue(
        KAFKA_PROVIDER == KafkaProvider.CONFLUENT, "Avro tests require Confluent Kafka provider");
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "Avro tests require distributed mode, otherwise Avro converter is not available");
  }

  @Override
  KafkaConsumer<GenericRecord, GenericRecord> buildConsumer(
      String connectorName, String tableName) {
    return buildAvroConnector(connectorName, tableName);
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
            "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
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
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
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
            "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
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
              "id": 1
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
              "frozen_tuple_col": {"value": {"tuple_member_0": 99, "tuple_member_1": "bar"}}
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
              "frozen_tuple_col": {"value": {"tuple_member_0": null, "tuple_member_1": null}}
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
              "frozen_tuple_col": {"value": null}
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
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
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
              "frozen_tuple_col": {"value": {"tuple_member_0": 42, "tuple_member_1": "foo"}}
            }
            """)
    };
  }
}
