package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildPlainConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesUDTPlainConnectorIT extends ScyllaTypesUDTBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildPlainConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsertWithFrozenUdt(TestInfo testInfo) {
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo) {
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
          .formatted(
              connectorName(testInfo),
              keyspaceName(testInfo),
              keyspaceName(testInfo),
              tableName(testInfo))
    };
  }

  @Override
  String[] expectedInsertWithNullUdt(TestInfo testInfo) {
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
  String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
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
  String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo) {
    return new String[] {
      expectedRecord(
          testInfo,
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
