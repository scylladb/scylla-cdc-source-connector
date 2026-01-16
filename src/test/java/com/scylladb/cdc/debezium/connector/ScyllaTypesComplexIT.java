package com.scylladb.cdc.debezium.connector;

/**
 * Integration tests for complex types (nested UDTs and collections) replication. Tests simple
 * insert operations with complex nested structures.
 */
@SuppressWarnings("rawtypes")
public abstract class ScyllaTypesComplexIT extends ScyllaTypesIT {
  //
  //  protected static void setupComplexTypesTable() {
  //    session.execute(
  //        "CREATE KEYSPACE IF NOT EXISTS complex_types_ks WITH replication = {'class':
  // 'SimpleStrategy', 'replication_factor': 1};");
  //    session.execute(
  //        "CREATE TYPE IF NOT EXISTS complex_types_ks.address_udt (street text, phones
  // frozen<list<text>>, tags frozen<set<text>>);");
  //    session.execute(
  //        "CREATE TABLE IF NOT EXISTS complex_types_ks.tab ("
  //            + "id int PRIMARY KEY,"
  //            + "frozen_addr frozen<address_udt>,"
  //            + "nf_addr address_udt,"
  //            + "frozen_addr_list frozen<list<frozen<address_udt>>>,"
  //            + "nf_addr_set set<frozen<address_udt>>,"
  //            + "nf_addr_map map<int, frozen<address_udt>>"
  //            + ") WITH cdc = {'enabled':true}");
  //    session.execute(
  //        "INSERT INTO complex_types_ks.tab (id, frozen_addr, nf_addr, frozen_addr_list,
  // nf_addr_set, nf_addr_map) VALUES ("
  //            + "1,"
  //            + "{street: 'main', phones: ['111','222'], tags: {'home','primary'}},"
  //            + "{street: 'side', phones: ['333'], tags: {'secondary'}},"
  //            + "[{street: 'l1', phones: ['444'], tags: {'list1'}}, {street: 'l2', phones:
  // ['555'], tags: {'list2'}}],"
  //            + "{{street: 's1', phones: ['666'], tags: {'tag1'}}, {street: 's2', phones: ['777'],
  // tags: {'tag2'}}},"
  //            + "{10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}, 20: {street: 'm2', phones:
  // ['999'], tags: {'tagm2'}}}"
  //            + ");");
  //  }
  //
  //  @Test
  //  public void canReplicateComplexUDTAndCollections() throws UnknownHostException {
  //    // Issue a series of UPDATEs that modify the non-frozen UDT and
  //    // non-frozen collections of UDTs. In practice Scylla CDC may emit
  //    // collection deltas (especially for sets/maps) in ways that are
  //    // hard to assert deterministically across versions and providers.
  //    //
  //    // This test therefore *requires* observing:
  //    //   - the CREATE (op="c") event with the full initial complex value
  //    //   - an UPDATE (op="u") event for the non-frozen UDT field nf_addr
  //    //
  //    // and only *optionally* asserts the detailed nf_addr_map delta if it
  //    // appears in the stream. We do not fail the test if no separate
  //    // nf_addr_map UPDATE event is observed.
  //    try (Cluster cluster =
  //            Cluster.builder()
  //                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
  //                .withPort(scyllaDBContainer.getMappedPort(9042))
  //                .build();
  //        Session session = cluster.connect()) {
  //      session.execute(
  //          "UPDATE complex_types_ks.tab SET "
  //              + "nf_addr.street = 'side-updated', "
  //              + "nf_addr.tags = null "
  //              + "WHERE id = 1;");
  //      session.execute(
  //          "UPDATE complex_types_ks.tab SET "
  //              + "nf_addr_set = nf_addr_set - {{street: 's1', phones: ['666'], tags: {'tag1'}}} "
  //              + "WHERE id = 1;");
  //      session.execute(
  //          "UPDATE complex_types_ks.tab SET "
  //              + "nf_addr_set = nf_addr_set + {{street: 's3', phones: ['000'], tags: {'tag3'}}} "
  //              + "WHERE id = 1;");
  //      session.execute(
  //          "UPDATE complex_types_ks.tab SET "
  //              + "nf_addr_map = nf_addr_map - {10} "
  //              + "WHERE id = 1;");
  //      session.execute(
  //          "UPDATE complex_types_ks.tab SET "
  //              + "nf_addr_map = nf_addr_map + {30: {street: 'm3', phones: ['123'], tags:
  // {'tagm3'}}} "
  //              + "WHERE id = 1;");
  //    }
  //
  //    final String COMPLEX_TYPES_CONNECTOR = "ComplexUDTAndCollectionsConnector";
  //    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
  //      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
  //      connectorConfiguration.put("topic.prefix", "canReplicateComplexUDTAndCollections");
  //      connectorConfiguration.put("scylla.table.names", "complex_types_ks.tab");
  //      connectorConfiguration.put("name", COMPLEX_TYPES_CONNECTOR);
  //      registerConnector(COMPLEX_TYPES_CONNECTOR, connectorConfiguration);
  //      consumer.subscribe(List.of("canReplicateComplexUDTAndCollections.complex_types_ks.tab"));
  //      long startTime = System.currentTimeMillis();
  //      boolean foundCreate = false;
  //      boolean foundUpdateNfAddr = false;
  //      boolean foundUpdateNfAddrMap = false;
  //
  //      while (System.currentTimeMillis() - startTime < CONSUMER_TIMEOUT
  //          && (!foundCreate || !foundUpdateNfAddr)) {
  //        var records = consumer.poll(Duration.ofSeconds(5));
  //        if (!records.isEmpty()) {
  //          for (var record : records) {
  //            String value = record.value();
  //            if (!value.contains("\"id\":1")) {
  //              continue;
  //            }
  //
  //            if (value.contains("\"op\":\"c\"") && !foundCreate) {
  //              /*
  //                Example message structure (partially formatted for convenience):
  //                {
  //                  "source": {
  //                      "version": "1.2.8-SNAPSHOT",
  //                      "connector": "scylla",
  //                      "name": "canReplicateComplexUDTAndCollections",
  //                      "ts_ms": 1767184765436,
  //                      "snapshot": "false",
  //                      "db": "complex_types_ks",
  //                      "sequence": null,
  //                      "ts_us": 1767184765436445,
  //                      "ts_ns": 1767184765436000000,
  //                      "keyspace_name": "complex_types_ks",
  //                      "table_name": "tab"
  //                  },
  //                  "before": null,
  //                  "after": {
  //                      "frozen_addr": {
  //                          "value": {
  //                              "street": "main",
  //                              "phones": ["111","222"],
  //                              "tags": ["home","primary"]
  //                          }
  //                      },
  //                      "frozen_addr_list": {
  //                          "value": [
  //                              {"street":"l1","phones":["444"],"tags":["list1"]},
  //                              {"street":"l2","phones":["555"],"tags":["list2"]}
  //                          ]
  //                      },
  //                      "id": 1,
  //                      "nf_addr": {
  //                          "value": {
  //                              "mode": "OVERWRITE",
  //                              "elements": {
  //                                  "street": {
  //                                      "value": "side"
  //                                  },
  //                                  "phones": {
  //                                      "value": ["333"]
  //                                  },
  //                                  "tags": {
  //                                      "value": ["secondary"]
  //                                  }
  //                              }
  //                          }
  //                      },
  //                      "nf_addr_map": {
  //                          "value": {
  //                              "mode": "OVERWRITE",
  //                              "elements": [
  //                                  {
  //                                      "key": 10,
  //                                      "value": {
  //                                          "street": "m1",
  //                                          "phones": ["888"],
  //                                          "tags": ["tagm1"]
  //                                      }
  //                                  },
  //                                  {
  //                                      "key": 20,
  //                                      "value": {
  //                                          "street": "m2",
  //                                          "phones": ["999"],
  //                                          "tags": ["tagm2"]
  //                                      }
  //                                  }
  //                              ]
  //                          }
  //                      },
  //                      "nf_addr_set": {
  //                          "value": {
  //                              "mode": "OVERWRITE",
  //                              "elements": [
  //                                  {
  //                                      "element": {
  //                                          "street": "s1",
  //                                          "phones": ["666"],
  //                                          "tags": ["tag1"]
  //                                      },
  //                                      "added": true
  //                                  },
  //                                  {
  //                                      "element": {
  //                                          "street": "s2",
  //                                          "phones": ["777"],
  //                                          "tags": ["tag2"]
  //                                      },
  //                                      "added": true
  //                                  }
  //                              ]
  //                          }
  //                      }
  //                  },
  //                  "op": "c",
  //                  "ts_ms": 1767184779770,
  //                  "transaction": null,
  //                  "ts_us": 1767184779770036,
  //                  "ts_ns": 1767184779770036000
  //                }
  //              */
  //              assertAll(
  //                  // frozen UDT with nested collections
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"frozen_addr\":{\"value\":{\"street\":\"main\""),
  //                          "Expected frozen_addr with street 'main' in value: " + value),
  //                  // non-frozen UDT initial state (OVERWRITE)
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"nf_addr\":{\"value\":{\"mode\":\"OVERWRITE\""),
  //                          "Expected nf_addr mode OVERWRITE in create value: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"street\":{\"value\":\"side\""),
  //                          "Expected nf_addr street 'side' in create value: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"phones\":{\"value\":[\"333\"]"),
  //                          "Expected nf_addr phones ['333'] in create value: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"tags\":{\"value\":[\"secondary\"]"),
  //                          "Expected nf_addr tags ['secondary'] in create value: " + value),
  //                  // frozen_addr_list
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"frozen_addr_list\":{\"value\":[")
  //                              && value.contains("\"street\":\"l1\"")
  //                              && value.contains("\"street\":\"l2\""),
  //                          "Expected frozen_addr_list with l1 and l2 in value: " + value),
  //                  // nf_addr_set initial state (OVERWRITE)
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"nf_addr_set\":{\"value\":{\"mode\":\"OVERWRITE\""),
  //                          "Expected nf_addr_set mode OVERWRITE in create value: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"street\":\"s1\"")
  //                              && value.contains("\"street\":\"s2\""),
  //                          "Expected nf_addr_set elements s1 and s2 in create value: " + value),
  //                  // nf_addr_map initial state (OVERWRITE)
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"nf_addr_map\":{\"value\":{\"mode\":\"OVERWRITE\""),
  //                          "Expected nf_addr_map mode OVERWRITE in create value: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("10") && value.contains("\"street\":\"m1\""),
  //                          "Expected nf_addr_map key 10 with street m1 in create value: " +
  // value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains("20") && value.contains("\"street\":\"m2\""),
  //                          "Expected nf_addr_map key 20 with street m2 in create value: " +
  // value));
  //              foundCreate = true;
  //            } else if (value.contains("\"op\":\"u\"")) {
  //              /*
  //                Example message structure (formatted for convenience):
  //                {
  //                  "source": {
  //                      "version": "1.2.8-SNAPSHOT",
  //                      "connector": "scylla",
  //                      "name": "canReplicateComplexUDTAndCollections",
  //                      "ts_ms": 1767184767506,
  //                      "snapshot": "false",
  //                      "db": "complex_types_ks",
  //                      "sequence": null,
  //                      "ts_us": 1767184767506903,
  //                      "ts_ns": 1767184767506000000,
  //                      "keyspace_name": "complex_types_ks",
  //                      "table_name": "tab"
  //                  },
  //                  "before": null,
  //                  "after": {
  //                      "frozen_addr": null,
  //                      "frozen_addr_list": null,
  //                      "id": 1,
  //                      "nf_addr": {
  //                          "value": {
  //                              "mode": "MODIFY",
  //                              "elements": {
  //                                  "street": {
  //                                      "value": "side-updated"
  //                                  },
  //                                  "phones": {
  //                                      "value": []
  //                                  },
  //                                  "tags": {
  //                                      "value": []
  //                                  }
  //                              }
  //                          }
  //                      },
  //                      "nf_addr_map": null,
  //                      "nf_addr_set": null
  //                  },
  //                  "op": "u",
  //                  "ts_ms": 1767184779779,
  //                  "transaction": null,
  //                  "ts_us": 1767184779779062,
  //                  "ts_ns": 1767184779779062000
  //                }
  //              */
  //              if (!foundUpdateNfAddr && value.contains("\"nf_addr\":{\"value\"")) {
  //                assertAll(
  //                    () ->
  //                        assertTrue(
  //                            value.contains("\"nf_addr\":{\"value\":{\"mode\":\"MODIFY\""),
  //                            "Expected nf_addr mode MODIFY in update value: " + value),
  //                    () ->
  //                        assertTrue(
  //                            value.contains("\"street\":{\"value\":\"side-updated\""),
  //                            "Expected nf_addr updated street in value: " + value),
  //                    () ->
  //                        assertTrue(
  //                            value.contains("\"tags\":{\"value\":[]}")
  //                                || value.contains("\"tags\":{\"value\":[ ]}"),
  //                            "Expected nf_addr tags updated to empty set in value: " + value));
  //                foundUpdateNfAddr = true;
  //              }
  //
  //              if (!foundUpdateNfAddrMap && value.contains("\"nf_addr_map\":{\"value\"")) {
  //                assertAll(
  //                    () ->
  //                        assertTrue(
  //                            value.contains("\"nf_addr_map\":{\"value\":{\"mode\":\"MODIFY\""),
  //                            "Expected nf_addr_map mode MODIFY in update value: " + value),
  //                    () ->
  //                        assertTrue(
  //                            value.contains("[10,null]") || value.contains("[10, null]"),
  //                            "Expected nf_addr_map removal of key 10 in value: " + value),
  //                    () ->
  //                        assertTrue(
  //                            value.contains("30") && value.contains("\"street\":\"m3\""),
  //                            "Expected nf_addr_map key 30 with street m3 in value: " + value));
  //                foundUpdateNfAddrMap = true;
  //              }
  //            }
  //          }
  //        }
  //      }
  //
  //      consumer.unsubscribe();
  //      assertTrue(
  //          foundCreate,
  //          "No CREATE event consumed for complex UDT/collections row. Topic may be empty or
  // connector may have crashed.");
  //      assertTrue(
  //          foundUpdateNfAddr,
  //          "No UPDATE event for nf_addr consumed for complex UDT/collections row.");
  //    }
  //  }
}
