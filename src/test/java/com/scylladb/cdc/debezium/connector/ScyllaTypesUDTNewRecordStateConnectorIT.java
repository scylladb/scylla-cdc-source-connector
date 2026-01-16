package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesUDTNewRecordStateConnectorIT extends ScyllaTypesUDTBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  void waitAndAssert(KafkaConsumer<String, String> consumer, String[] expected) {
    waitAndAssertKafkaMessages(consumer, expected);
  }

  @Override
  String[] expectedInsertWithFrozenUdt() {
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
  String[] expectedInsertWithNonFrozenUdt() {
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
  String[] expectedInsertWithNullUdt() {
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
  String[] expectedDelete() {
    return new String[] {
      """
        {
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFrozenUdtFromValueToValue() {
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
  String[] expectedUpdateFrozenUdtFromValueToNull() {
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
  String[] expectedUpdateNonFrozenUdtField() {
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
