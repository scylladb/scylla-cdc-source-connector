package com.scylladb.cdc.debezium.connector;

/**
 * Integration tests for UDT (User-Defined Types) replication. Tests simple insert operations with
 * both frozen and non-frozen UDTs.
 */
@SuppressWarnings("rawtypes")
public abstract class ScyllaTypesUDTIT extends ScyllaTypesIT {
  //
  //  protected static void setupUDTTable(Session session) {
  //    session.execute(
  //        "CREATE KEYSPACE IF NOT EXISTS udt_ks WITH replication = {'class': 'SimpleStrategy',
  // 'replication_factor': 1};");
  //    session.execute("CREATE TYPE IF NOT EXISTS udt_ks.simple_udt (a int, b text);");
  //    session.execute(
  //        "CREATE TABLE IF NOT EXISTS udt_ks.tab ("
  //            + "id int PRIMARY KEY,"
  //            + "udt_col frozen<simple_udt>,"
  //            + "nf_udt_col simple_udt"
  //            + ") WITH cdc = {'enabled':true}");
  //    // Insert row with non-null UDTs
  //    session.execute(
  //        "INSERT INTO udt_ks.tab (id, udt_col, nf_udt_col) VALUES (1, {a: 42, b: 'foo'}, {a: 7,
  // b: 'bar'});");
  //    // Insert row with null UDTs
  //    session.execute("INSERT INTO udt_ks.tab (id, udt_col, nf_udt_col) VALUES (2, null, null);");
  //  }
  //
  //  @Test
  //  public void canReplicateUDT() throws UnknownHostException, Exception {
  //    final String UDT_CONNECTOR = "UDTConnector";
  //    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
  //      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
  //      connectorConfiguration.put("topic.prefix", "canReplicateUDT");
  //      connectorConfiguration.put("scylla.table.names", "udt_ks.tab");
  //      connectorConfiguration.put("name", UDT_CONNECTOR);
  //      registerConnector(connectorConfiguration, UDT_CONNECTOR);
  //      consumer.subscribe(List.of("canReplicateUDT.udt_ks.tab"));
  //      long startTime = System.currentTimeMillis();
  //      boolean foundNonNull = false;
  //      boolean foundNull = false;
  //      while (System.currentTimeMillis() - startTime < CONSUMER_TIMEOUT
  //          && (!foundNonNull || !foundNull)) {
  //        var records = consumer.poll(java.time.Duration.ofSeconds(5));
  //        if (!records.isEmpty()) {
  //          for (var record : records) {
  //            String value = record.value();
  //            if (value.contains("\"id\":1")) {
  //              /*
  //               Example message structure (partially formatted for convenience):
  //               {
  //                 "source": {
  //                     "version": "1.2.8-SNAPSHOT",
  //                     "connector": "scylla",
  //                     "name": "canReplicateUDT",
  //                     "ts_ms": 1767184281099,
  //                     "snapshot": "false",
  //                     "db": "udt_ks",
  //                     "sequence": null,
  //                     "ts_us": 1767184281099870,
  //                     "ts_ns": 1767184281099000000,
  //                     "keyspace_name": "udt_ks",
  //                     "table_name": "tab"
  //                 },
  //                 "before": null,
  //                 "after": {
  //                     "id": 1,
  //                     "nf_udt_col": {
  //                         "value": {
  //                             "mode": "OVERWRITE",
  //                             "elements": {"a":{"value":7},"b":{"value":"bar"}}
  //                         }
  //                     },
  //                     "udt_col": {
  //                         "value":{"a":42,"b":"foo"}}
  //                     }
  //                 },
  //                 "op": "c",
  //                 "ts_ms": 1767184295461,
  //                 "transaction": null,
  //                 "ts_us": 1767184295461558,
  //                 "ts_ns": 1767184295461558000
  //               }
  //              */
  //              assertAll(
  //                  // frozen UDT
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"udt_col\":{\"value\":{\"a\":42,\"b\":\"foo\"}}"),
  //                          "Expected non-null frozen UDT value: " + value),
  //                  // non-frozen UDT (mode/OVERWRITE, elements with value fields)
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"nf_udt_col\":{\"value\":{\"mode\":\"OVERWRITE\""),
  //                          "Expected mode=OVERWRITE for non-frozen UDT: " + value),
  //                  () ->
  //                      assertTrue(
  //                          value.contains(
  //                              "\"elements\":{\"a\":{\"value\":7},\"b\":{\"value\":\"bar\"}}"),
  //                          "Expected elements with correct values for non-frozen UDT: " +
  // value));
  //              foundNonNull = true;
  //            } else if (value.contains("\"id\":2")) {
  //              assertAll(
  //                  /* Frozen UDT
  //                    {
  //                      "source": {
  //                          "version": "1.2.8-SNAPSHOT",
  //                          "connector": "scylla",
  //                          "name": "canReplicateUDT",
  //                          "ts_ms": 1767184281100,
  //                          "snapshot": "false",
  //                          "db": "udt_ks",
  //                          "sequence": null,
  //                          "ts_us": 1767184281100749,
  //                          "ts_ns": 1767184281100000000,
  //                          "keyspace_name": "udt_ks",
  //                          "table_name": "tab"
  //                      },
  //                      "before": null,
  //                      "after": {
  //                          "id": 2,
  //                          "nf_udt_col": null,
  //                          "udt_col": {
  //                              "value": null
  //                          }
  //                      },
  //                      "op": "c",
  //                      "ts_ms": 1767184295471,
  //                      "transaction": null,
  //                      "ts_us": 1767184295471972,
  //                      "ts_ns": 1767184295471972000
  //                    }
  //                  */
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"udt_col\":{\"value\":null}"),
  //                          "Expected null frozen UDT value: " + value),
  //                  // non-frozen UDT
  //                  () ->
  //                      assertTrue(
  //                          value.contains("\"nf_udt_col\":null")
  //                              || value.contains("\"nf_udt_col\":{\"value\":null}"),
  //                          "Expected null non-frozen UDT value (top-level null or {value:null}):
  // "
  //                              + value));
  //              foundNull = true;
  //            }
  //          }
  //        }
  //      }
  //      consumer.unsubscribe();
  //      assertTrue(foundNonNull, "No message consumed for non-null UDT row.");
  //      assertTrue(foundNull, "No message consumed for null UDT row.");
  //    }
  //  }
}
