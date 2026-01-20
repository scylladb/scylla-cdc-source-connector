package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesUDTNewRecordStateConnectorIT extends ScyllaTypesUDTBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsertWithFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_udt": {"a": 42, "b": "foo"},
          "nf_udt": null
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithNonFrozenUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_udt": null,
          "nf_udt": {"a": 7, "b": "bar"}
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithNullUdt(TestInfo testInfo) {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_udt": null,
          "nf_udt": null
        }
        """
    };
  }

  @Override
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_udt": {"a": 99, "b": "updated"}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFrozenUdtFromValueToNull(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_udt": null
        }
        """
    };
  }

  @Override
  String[] expectedUpdateNonFrozenUdtField(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "nf_udt": {"a": 100}
        }
        """
    };
  }
}
