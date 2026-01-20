package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildAvroConnector;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesComplexAvroConnectorIT
    extends ScyllaTypesComplexBase<GenericRecord, GenericRecord> {

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
  String[] expectedInsertWithAllTypes(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_addr": {
              "value": {
                "street": "main",
                "phones": ["111", "222"],
                "tags": ["home", "primary"]
              }
            },
            "nf_addr": {
              "value": {
                "mode": "OVERWRITE",
                "elements": {
                  "street": {"value": "side"},
                  "phones": {"value": ["333"]},
                  "tags": {"value": ["secondary"]}
                }
              }
            },
            "frozen_addr_list": {
              "value": [
                {"street": "l1", "phones": ["444"], "tags": ["list1"]}
              ]
            },
            "nf_addr_set": {
              "value": {
                "mode": "OVERWRITE",
                "elements": [
                  {
                    "element": {"street": "s1", "phones": ["666"], "tags": ["tag1"]},
                    "added": true
                  }
                ]
              }
            },
            "nf_addr_map": {
              "value": {
                "mode": "OVERWRITE",
                "elements": [
                  {
                    "key": 10,
                    "value": {"street": "m1", "phones": ["888"], "tags": ["tagm1"]}
                  }
                ]
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedInsertWithNullTypes(TestInfo testInfo) {
    return new String[] {
      """
        {
          "before": null,
          "after": {
            "id": 1,
            "frozen_addr": {"value": null},
            "nf_addr": null,
            "frozen_addr_list": {"value": null},
            "nf_addr_set": null,
            "nf_addr_map": null
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
  String[] expectedUpdateFrozenAddr(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "frozen_addr": {
                "value": {
                  "street": "updated",
                  "phones": ["999"],
                  "tags": ["new"]
                }
              }
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrField(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "nf_addr": {
                "value": {
                  "mode": "MODIFY",
                  "elements": {
                    "street": {"value": "modified"}
                  }
                }
              }
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrSet(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "nf_addr_set": {
                "value": {
                  "mode": "MODIFY",
                  "elements": [
                    {
                      "element": {"street": "s2", "phones": ["777"], "tags": ["tag2"]},
                      "added": true
                    }
                  ]
                }
              }
            }
            """)
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrMap(TestInfo testInfo) {
    return new String[] {
      expectedRecord(testInfo, "c", "null", "{}"),
      expectedRecord(
          testInfo,
          "u",
          "null",
          """
            {
              "id": 1,
              "nf_addr_map": {
                "value": {
                  "mode": "MODIFY",
                  "elements": [
                    {
                      "key": 20,
                      "value": {"street": "m2", "phones": ["999"], "tags": ["tagm2"]}
                    }
                  ]
                }
              }
            }
            """)
    };
  }
}
