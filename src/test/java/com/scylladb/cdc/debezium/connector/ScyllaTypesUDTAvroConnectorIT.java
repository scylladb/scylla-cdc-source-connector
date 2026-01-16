package com.scylladb.cdc.debezium.connector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;

public class ScyllaTypesUDTAvroConnectorIT
    extends ScyllaTypesUDTBase<GenericRecord, GenericRecord> {

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
  String[] expectedInsertWithFrozenUdt() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": {"a": 42, "b": "foo"}},
            "nf_udt": null
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
  String[] expectedInsertWithNonFrozenUdt() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": null},
            "nf_udt": {
              "value": {
                "mode": "OVERWRITE",
                "elements": {"a": {"value": 7}, "b": {"value": "bar"}}
              }
            }
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
  String[] expectedInsertWithNullUdt() {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_udt": {"value": null},
            "nf_udt": null
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
  String[] expectedUpdateFrozenUdtFromValueToValue() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_udt": {"value": {"a": 99, "b": "updated"}}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_udt": {"value": null}
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateNonFrozenUdtField() {
    return new String[] {
      expectedRecord("c", "null", "{}"),
      expectedRecord(
          "u",
          "null",
          """
            {
              "id": 1,
              "nf_udt": {
                "value": {
                  "mode": "MODIFY",
                  "elements": {"a": {"value": 100}}
                }
              }
            }
            """)
    };
  }
}
