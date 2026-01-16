package com.scylladb.cdc.debezium.connector;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ScyllaTypesComplexNewRecordStateConnectorIT
    extends ScyllaTypesComplexBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  void waitAndAssert(KafkaConsumer<String, String> consumer, String[] expected) {
    waitAndAssertKafkaMessages(consumer, expected);
  }

  @Override
  String[] expectedInsertWithAllTypes() {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_addr": {
            "street": "main",
            "phones": ["111", "222"],
            "tags": ["home", "primary"]
          },
          "nf_addr": {
            "street": "side",
            "phones": ["333"],
            "tags": ["secondary"]
          },
          "frozen_addr_list": [
            {"street": "l1", "phones": ["444"], "tags": ["list1"]}
          ],
          "nf_addr_set": [
            {"street": "s1", "phones": ["666"], "tags": ["tag1"]}
          ],
          "nf_addr_map": [
            [10, {"street": "m1", "phones": ["888"], "tags": ["tagm1"]}]
          ]
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithNullTypes() {
    return new String[] {
      """
        {
          "id": 1,
          "frozen_addr": null,
          "nf_addr": null,
          "frozen_addr_list": null,
          "nf_addr_set": null,
          "nf_addr_map": null
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
  String[] expectedUpdateFrozenAddr() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "frozen_addr": {
            "street": "updated",
            "phones": ["999"],
            "tags": ["new"]
          }
        }
        """
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrField() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "nf_addr": {"street": "modified"}
        }
        """
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrSet() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "nf_addr_set": [
            {"street": "s2", "phones": ["777"], "tags": ["tag2"]}
          ]
        }
        """
    };
  }

  @Override
  String[] expectedUpdateNonFrozenAddrMap() {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "nf_addr_map": [
            [20, {"street": "m2", "phones": ["999"], "tags": ["tagm2"]}]
          ]
        }
        """
    };
  }
}
