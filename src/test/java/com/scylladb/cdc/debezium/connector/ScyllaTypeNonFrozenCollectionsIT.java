package com.scylladb.cdc.debezium.connector;

/**
 * Integration tests for non-frozen collections replication. Tests simple insert operations with
 * non-frozen collections.
 */
// public class ScyllaTypeNonFrozenCollectionsIT extends ScyllaTypesIT {
//  final static String CONNECTOR_NAME = ScyllaTypeNonFrozenCollectionsIT.class.getName();
//
//  @BeforeAll
//  protected static void setup() throws Exception {
//    session.execute(
//        "CREATE KEYSPACE IF NOT EXISTS nonfrozen_collections_ks WITH replication = {'class':
// 'SimpleStrategy', 'replication_factor': 1};");
//    session.execute(
//        "CREATE TABLE IF NOT EXISTS nonfrozen_collections_ks.tab ("
//            + "id int PRIMARY KEY,"
//            + "list_col list<int>,"
//            + "set_col set<text>,"
//            + "map_col map<int, text>"
//            + ") WITH cdc = {'enabled':true}");
//    session.execute(
//        "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES ("
//            + "1, [10,20,30], {'x','y','z'}, {10:'ten',20:'twenty'}"
//            + ");");
//
//    consumer = KafkaUtils.createStringConsumer();
//    Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
//    connectorConfiguration.put("topic.prefix", "canReplicateNonFrozenCollections");
//    connectorConfiguration.put("scylla.table.names", "nonfrozen_collections_ks.tab");
//    connectorConfiguration.put("name", CONNECTOR_NAME);
//    registerConnector(connectorConfiguration, CONNECTOR_NAME);
//  }
//
//  @AfterAll
//  static void cleanup() {
//    if (consumer != null) {
//      consumer.close();
//    }
//  }
//
//
//    @Test
//  public void canReplicateNonFrozenCollections() throws UnknownHostException {
//    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
//
// consumer.subscribe(List.of("canReplicateNonFrozenCollections.nonfrozen_collections_ks.tab"));
//      long startTime = System.currentTimeMillis();
//      boolean messageConsumed = false;
//      while (System.currentTimeMillis() - startTime < CONSUMER_TIMEOUT) {
//        var records = consumer.poll(java.time.Duration.ofSeconds(5));
//        if (!records.isEmpty()) {
//          for (var record : records) {
//            String value = record.value();
//            /*
//              Below is an example of expected message structure. Note that JSON is formatted for
// readability except for value fields:
//              {
//                "source": {
//                    "version": "1.2.8-SNAPSHOT",
//                    "connector": "scylla",
//                    "name": "canReplicateNonFrozenCollections",
//                    "ts_ms": 1767182817995,
//                    "snapshot": "false",
//                    "db": "nonfrozen_collections_ks",
//                    "sequence": null,
//                    "ts_us": 1767182817995121,
//                    "ts_ns": 1767182817995000000,
//                    "keyspace_name": "nonfrozen_collections_ks",
//                    "table_name": "tab"
//                },
//                "before": null,
//                "after": {
//                    "id": 1,
//                    "list_col": {
//                        "value": {
//                            "mode": "OVERWRITE",
//                            "elements": [
//                                {
//                                    "key": "34b3de6a-e641-11f0-8080-d26e1c253f53",
//                                    "value": 10
//                                },
//                                {
//                                    "key": "34b3de6a-e641-11f0-8081-d26e1c253f53",
//                                    "value": 20
//                                },
//                                {
//                                    "key": "34b3de6a-e641-11f0-8082-d26e1c253f53",
//                                    "value": 30
//                                }
//                            ]
//                        }
//                    },
//                    "map_col": {
//                        "value": {
//                            "mode": "OVERWRITE",
//                            "elements": [
//                                {
//                                    "key": 10,
//                                    "value": "ten"
//                                },
//                                {
//                                    "key": 20,
//                                    "value": "twenty"
//                                }
//                            ]
//                        }
//                    },
//                    "set_col": {
//                        "value": {
//                            "mode": "OVERWRITE",
//                            "elements": [
//                                {
//                                    "element": "x",
//                                    "added": true
//                                },
//                                {
//                                    "element": "y",
//                                    "added": true
//                                },
//                                {
//                                    "element": "z",
//                                    "added": true
//                                }
//                            ]
//                        }
//                    }
//                },
//                "op": "c",
//                "ts_ms": 1767182832406,
//                "transaction": null,
//                "ts_us": 1767182832406881,
//                "ts_ns": 1767182832406881000
//            }
//               */
//            if (value.contains("\"id\":1")) {
//              // list_col: mode OVERWRITE, values {10,20,30} regardless of internal keys.
//              JsonNode listValue = KafkaUtils.extractValueNodeFromAfterField(value, "list_col");
//              JsonNode listElements = listValue.path("elements");
//              var listValues = new HashSet<Integer>();
//              assertEquals(
//                  "OVERWRITE",
//                  listValue.path("mode").asText(),
//                  "Expected list_col delta mode OVERWRITE in value: " + value);
//              assertTrue(
//                  listElements.isArray(), "Expected list_col elements array in value: " + value);
//              listElements
//                  .elements()
//                  .forEachRemaining(entry -> listValues.add(entry.path("value").asInt()));
//              assertEquals(
//                  Set.of(10, 20, 30),
//                  listValues,
//                  "Unexpected list_col elements in value: " + value);
//
//              // set_col: mode OVERWRITE, elements {x,y,z} as added elements.
//              JsonNode setValue = KafkaUtils.extractValueNodeFromAfterField(value, "set_col");
//              JsonNode setElements = setValue.path("elements");
//              var setValues = new HashSet<String>();
//              assertEquals(
//                  "OVERWRITE",
//                  setValue.path("mode").asText(),
//                  "Expected set_col delta mode OVERWRITE in value: " + value);
//              assertTrue(
//                  setElements.isArray(), "Expected set_col elements array in value: " + value);
//              setElements
//                  .elements()
//                  .forEachRemaining(
//                      entry -> {
//                        if (entry.path("added").asBoolean()) {
//                          setValues.add(entry.path("element").asText());
//                        }
//                      });
//              assertEquals(
//                  Set.of("x", "y", "z"),
//                  setValues,
//                  "Unexpected set_col elements in value: " + value);
//
//              // map_col: mode OVERWRITE, entries {10:"ten", 20:"twenty"}.
//              JsonNode mapValue = KafkaUtils.extractValueNodeFromAfterField(value, "map_col");
//              JsonNode mapElements = mapValue.path("elements");
//              assertEquals(
//                  "OVERWRITE",
//                  mapValue.path("mode").asText(),
//                  "Expected map_col delta mode OVERWRITE in value: " + value);
//              assertTrue(
//                  mapElements.isArray(), "Expected map_col elements array in value: " + value);
//              var mapEntries =
//                  StreamSupport.stream(mapElements.spliterator(), false)
//                      .collect(
//                          Collectors.toMap(
//                              e -> e.path("key").asInt(), e -> e.path("value").asText()));
//              assertEquals(
//                  2, mapEntries.size(), "Expected exactly 2 entries in map_col elements: " +
// value);
//              assertEquals(
//                  "ten", mapEntries.get(10), "Expected map_col entry 10:'ten' in value: " +
// value);
//              assertEquals(
//                  "twenty",
//                  mapEntries.get(20),
//                  "Expected map_col entry 20:'twenty' in value: " + value);
//              messageConsumed = true;
//              break;
//            }
//          }
//          if (messageConsumed) break;
//        }
//      }
//      consumer.unsubscribe();
//      assertTrue(messageConsumed, "No message consumed for non-frozen collections row.");
//    }
//  }
//
//  @Test
//  public void canReplicateNonFrozenCollectionsEdgeCases() throws UnknownHostException {
//    // Insert rows with empty, null, and element removal for non-frozen collections.
//    //
//    // NOTE: For non-frozen collections in CDC "delta" mode, Scylla does not
//    // provide enough information to distinguish an explicit empty collection
//    // value ([], {}, {}) from an explicit NULL on INSERT when there are no
//    // element-level deltas. Both cases surface as a "deleted" collection with
//    // no elements in the CDC log. Because of this, the connector intentionally
//    // treats both of these INSERT patterns as a top-level null for the
//    // non-frozen collection column. This test asserts that behavior.
//    try (Cluster cluster =
//            Cluster.builder()
//                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
//                .withPort(scyllaDBContainer.getMappedPort(9042))
//                .build();
//        Session session = cluster.connect()) {
//      // Empty collections
//      session.execute(
//          "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES (2,
// [], {}, {});");
//      // Null collections
//      session.execute(
//          "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES (3,
// null, null, null);");
//      // Perform simultaneous element removal and addition on each non-frozen collection
//      // in a single UPDATE statement to exercise combined collection deltas.
//      session.execute(
//          "UPDATE nonfrozen_collections_ks.tab SET "
//              + "list_col = list_col - [10], list_col = list_col + [40], "
//              + "set_col = set_col - {'x'}, set_col = set_col + {'w'}, "
//              + "map_col = map_col - {10}, map_col = map_col + {30:'thirty'} "
//              + "WHERE id = 1;");
//    }
//
//    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
//      final String NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR =
//          "NonFrozenCollectionsEdgeCasesConnector";
//      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
//      connectorConfiguration.put("topic.prefix", "canReplicateNonFrozenCollectionsEdgeCases");
//      connectorConfiguration.put("scylla.table.names", "nonfrozen_collections_ks.tab");
//      connectorConfiguration.put("name", NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR);
//      registerConnector(NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR, connectorConfiguration);
//      consumer.subscribe(
//          List.of("canReplicateNonFrozenCollectionsEdgeCases.nonfrozen_collections_ks.tab"));
//      long startTime = System.currentTimeMillis();
//      boolean foundEmpty = false;
//      boolean foundNull = false;
//      boolean foundRemoval = false;
//      while (System.currentTimeMillis() - startTime < CONSUMER_TIMEOUT
//          && (!foundEmpty || !foundNull || !foundRemoval)) {
//        var records = consumer.poll(java.time.Duration.ofSeconds(5));
//        if (!records.isEmpty()) {
//          for (var record : records) {
//            String value = record.value();
//            if (value.contains("\"id\":2")) {
//              /*
//                Empty collections. Due to CDC limitations described above,
//                these appear the same as explicit NULL collections on INSERT
//                for non-frozen types, so we expect top-level nulls.
//
//                Example message structure (formatted for readability):
//                {
//                  "source": {
//                      "version": "1.2.8-SNAPSHOT",
//                      "connector": "scylla",
//                      "name": "canReplicateNonFrozenCollectionsEdgeCases",
//                      "ts_ms": 1767183798577,
//                      "snapshot": "false",
//                      "db": "nonfrozen_collections_ks",
//                      "sequence": null,
//                      "ts_us": 1767183798577692,
//                      "ts_ns": 1767183798577000000,
//                      "keyspace_name": "nonfrozen_collections_ks",
//                      "table_name": "tab"
//                  },
//                  "before": null,
//                  "after": {
//                      "id": 3,
//                      "list_col": null,
//                      "map_col": null,
//                      "set_col": null
//                  },
//                  "op": "c",
//                  "ts_ms": 1767183810809,
//                  "transaction": null,
//                  "ts_us": 1767183810809356,
//                  "ts_ns": 1767183810809356000
//                }
//              */
//              assertAll(
//                  () ->
//                      assertTrue(
//                          value.contains("\"list_col\":null"),
//                          "Expected null list_col in value for empty collection row: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"set_col\":null"),
//                          "Expected null set_col in value for empty collection row: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"map_col\":null"),
//                          "Expected null map_col in value for empty collection row: " + value));
//              foundEmpty = true;
//            } else if (value.contains("\"id\":3")) {
//              /*
//                Null collections.
//
//                Example message structure (formatted for readability):
//                {
//                  "source": {
//                      "version": "1.2.8-SNAPSHOT",
//                      "connector": "scylla",
//                      "name": "canReplicateNonFrozenCollectionsEdgeCases",
//                      "ts_ms": 1767183798576,
//                      "snapshot": "false",
//                      "db": "nonfrozen_collections_ks",
//                      "sequence": null,
//                      "ts_us": 1767183798576619,
//                      "ts_ns": 1767183798576000000,
//                      "keyspace_name": "nonfrozen_collections_ks",
//                      "table_name": "tab"
//                  },
//                  "before": null,
//                  "after": {
//                      "id": 2,
//                      "list_col": null,
//                      "map_col": null,
//                      "set_col": null
//                  },
//                  "op": "c",
//                  "ts_ms": 1767183810813,
//                  "transaction": null,
//                  "ts_us": 1767183810813043,
//                  "ts_ns": 1767183810813043000
//                }
//              */
//              assertAll(
//                  () ->
//                      assertTrue(
//                          value.contains("\"list_col\":null"),
//                          "Expected null list_col in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"set_col\":null"),
//                          "Expected null set_col in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"map_col\":null"),
//                          "Expected null map_col in value: " + value));
//              foundNull = true;
//            } else if (value.contains("\"id\":1") && value.contains("\"op\":\"u\"")) {
//              /*
//                Element removal and addition (UPDATE event only; skip initial CREATE).
//
//                Example message structure (partially formatted for readability:
//                {
//                  "source": {
//                      "version": "1.2.8-SNAPSHOT",
//                      "connector": "scylla",
//                      "name": "canReplicateNonFrozenCollectionsEdgeCases",
//                      "ts_ms": 1767183798578,
//                      "snapshot": "false",
//                      "db": "nonfrozen_collections_ks",
//                      "sequence": null,
//                      "ts_us": 1767183798578421,
//                      "ts_ns": 1767183798578000000,
//                      "keyspace_name": "nonfrozen_collections_ks",
//                      "table_name": "tab"
//                  },
//                  "before": null,
//                  "after": {
//                      "id": 1,
//                      "list_col": {
//                          "value": {
//                              "mode": "MODIFY",
//                              "elements":
// [{"key":"7d2d0192-e643-11f0-8080-3b6a81222237","value":40},{"key":"7bd9642a-e643-11f0-8080-3b6a81222237","value":null}]
//                          }
//                      },
//                      "map_col": {
//                          "value": {
//                              "mode": "MODIFY",
//                              "elements": [{"key":30,"value":"thirty"},{"key":10,"value":null}]
//                          }
//                      },
//                      "set_col": {
//                          "value": {
//                              "mode": "MODIFY",
//                              "elements":
// [{"element":"w","added":true},{"element":"x","added":false}]
//                          }
//                      }
//                  },
//                  "op": "u",
//                  "ts_ms": 1767183810816,
//                  "transaction": null,
//                  "ts_us": 1767183810816186,
//                  "ts_ns": 1767183810816186000
//                }
//              */
//              assertAll(
//                  () ->
//                      assertTrue(
//                          value.contains("\"list_col\":{\"value\":{\"mode\":\"MODIFY\""),
//                          "Expected list_col delta mode MODIFY in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"set_col\":{\"value\":{\"mode\":\"MODIFY\""),
//                          "Expected set_col delta mode MODIFY in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"map_col\":{\"value\":{\"mode\":\"MODIFY\""),
//                          "Expected map_col delta mode MODIFY in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("\"elements\":"),
//                          "Expected elements field in delta in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("40"),
//                          "Expected list_col addition of 40 in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("w"),
//                          "Expected set_col addition of 'w' in value: " + value),
//                  () ->
//                      assertTrue(
//                          value.contains("thirty"),
//                          "Expected map_col addition of key 30:'thirty' in value: " + value));
//              foundRemoval = true;
//            }
//          }
//        }
//      }
//      consumer.unsubscribe();
//      assertTrue(foundEmpty, "No message consumed for empty non-frozen collections row.");
//      assertTrue(foundNull, "No message consumed for null non-frozen collections row.");
//      assertTrue(
//          foundRemoval, "No message consumed for element removal in non-frozen collections row.");
//    }
//  }
// }
