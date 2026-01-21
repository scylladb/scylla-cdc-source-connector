package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.KafkaConnectUtils.buildScyllaExtractNewRecordStateConnector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.TestInfo;

public class ScyllaTypesComplexNewRecordStateConnectorIT
    extends ScyllaTypesComplexBase<String, String> {
  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildScyllaExtractNewRecordStateConnector(connectorName, tableName);
  }

  @Override
  String[] expectedInsertWithAllTypes(TestInfo testInfo) {
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
            {"key": 10, "value": {"street": "m1", "phones": ["888"], "tags": ["tagm1"]}}
          ]
        }
        """
    };
  }

  @Override
  String[] expectedInsertWithNullTypes(TestInfo testInfo) {
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
  String[] expectedDelete(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """
    };
  }

  @Override
  String[] expectedUpdateFrozenAddr(TestInfo testInfo) {
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
  String[] expectedUpdateNonFrozenAddrField(TestInfo testInfo) {
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
  String[] expectedUpdateNonFrozenAddrSet(TestInfo testInfo) {
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
  String[] expectedUpdateNonFrozenAddrMap(TestInfo testInfo) {
    return new String[] {
      """
        {
        }
        """,
      """
        {
          "id": 1,
          "nf_addr_map": [
            {"key": 20, "value": {"street": "m2", "phones": ["999"], "tags": ["tagm2"]}}
          ]
        }
        """
    };
  }
}
