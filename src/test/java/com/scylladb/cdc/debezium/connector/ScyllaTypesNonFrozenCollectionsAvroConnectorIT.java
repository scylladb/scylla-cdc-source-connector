package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesNonFrozenCollectionsAvroConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<GenericRecord, GenericRecord> {

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
            "list_col": {"value": {"mode": "OVERWRITE", "elements": [{"value": 10}, {"value": 20}, {"value": 30}]}},
            "set_col": {"value": {"mode": "OVERWRITE", "elements": [{"element": "x", "added": true}, {"element": "y", "added": true}, {"element": "z", "added": true}]}},
            "map_col": {"value": {"mode": "OVERWRITE", "elements": [{"key": 10, "value": "ten"}, {"key": 20, "value": "twenty"}]}}
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
            "list_col": {"value": null},
            "set_col": {"value": null},
            "map_col": {"value": null}
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
  String[] expectedUpdateListAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": {"mode": "MODIFY", "elements": [{"value": 30}]}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateSetAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "set_col": {"value": {"mode": "MODIFY", "elements": [{"element": "z", "added": true}]}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateMapAddElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "map_col": {"value": {"mode": "MODIFY", "elements": [{"key": 20, "value": "twenty"}]}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateListRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": null
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateSetRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "set_col": null
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateMapRemoveElement(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "map_col": null
            }
            """)
    };
  }
}
