package com.scylladb.cdc.debezium.connector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

public class ScyllaTypesNonFrozenCollectionsAvroConnectorIT
    extends ScyllaTypesNonFrozenCollectionsBase<GenericRecord, GenericRecord> {

  @BeforeAll
  static void checkKafkaProvider() {
    Assumptions.assumeTrue(
        KAFKA_PROVIDER == KafkaProvider.CONFLUENT, "Avro tests require Confluent Kafka provider");
  }

  @Override
  KafkaConsumer<GenericRecord, GenericRecord> buildConsumer(
      String connectorName, String tableName) {
    return buildAvroConnector(connectorName, tableName);
  }

  @Override
  void waitAndAssert(KafkaConsumer<GenericRecord, GenericRecord> consumer, String[] expected) {
    waitAndAssertAvroKafkaMessages(consumer, expected);
  }

  @Override
  String[] expectedInsertWithValues() {
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
          .formatted(connectorName(), KEYSPACE, KEYSPACE, tableNameOnly())
    };
  }

  @Override
  String[] expectedInsertWithNull() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "list_col": null,
            "set_col": null,
            "map_col": null
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
          .formatted(connectorName(), KEYSPACE, KEYSPACE, tableNameOnly())
    };
  }

  @Override
  String[] expectedDelete() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
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
  String[] expectedUpdateListAddElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
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
  String[] expectedUpdateSetAddElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
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
  String[] expectedUpdateMapAddElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
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
  String[] expectedUpdateListRemoveElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "list_col": {"value": {"mode": "MODIFY", "elements": [{"value": null}]}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateSetRemoveElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "set_col": {"value": {"mode": "MODIFY", "elements": [{"element": "y", "added": false}]}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateMapRemoveElement() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "map_col": {"value": {"mode": "MODIFY", "elements": [{"key": 10, "value": null}]}}
            }
            """)
    };
  }
}
