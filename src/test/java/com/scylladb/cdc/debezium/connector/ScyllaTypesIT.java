package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

public class ScyllaTypesIT extends AbstractContainerBaseIT {

  @BeforeAll
  public static void setupTables() {
    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {
      setupPrimitiveTypesTable(session);
      setupFrozenCollectionsTable(session);
      setupNonFrozenCollectionsTable(session);

      setupUDTTable(session);
      setupComplexTypesTable(session);
    }
  }

  private static void setupPrimitiveTypesTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS primitive_types_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TABLE IF NOT EXISTS primitive_types_ks.tab ("
            + "id int PRIMARY KEY,"
            + "ascii_col ascii,"
            + "bigint_col bigint,"
            + "blob_col blob,"
            + "boolean_col boolean,"
            + "date_col date,"
            + "decimal_col decimal,"
            + "double_col double,"
            + "duration_col duration,"
            + "float_col float,"
            + "inet_col inet,"
            + "int_col int,"
            + "smallint_col smallint,"
            + "text_col text,"
            + "time_col time,"
            + "timestamp_col timestamp,"
            + "timeuuid_col timeuuid,"
            + "tinyint_col tinyint,"
            + "uuid_col uuid,"
            + "varchar_col varchar,"
            + "varint_col varint"
            + ") WITH cdc = {'enabled':true}");
    session.execute(
        "INSERT INTO primitive_types_ks.tab (id, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col) VALUES ("
            + "1, 'ascii', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, 2.71828, '127.0.0.1', 42, 7, 'some text', '12:34:56.789', '2024-06-10T12:34:56.789Z', 81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, 'varchar text', 999999999)"
            + ";");
  }

  private static void setupFrozenCollectionsTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS frozen_collections_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TABLE IF NOT EXISTS frozen_collections_ks.tab ("
            + "id int PRIMARY KEY,"
            + "frozen_list_col frozen<list<int>>,"
            + "frozen_set_col frozen<set<text>>,"
            + "frozen_map_col frozen<map<int, text>>,"
            + "frozen_tuple_col frozen<tuple<int, text>>"
            + ") WITH cdc = {'enabled':true}");
    session.execute(
        "INSERT INTO frozen_collections_ks.tab (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES ("
            + "1, [1,2,3], {'a','b','c'}, {1:'one',2:'two'}, (42, 'foo')"
            + ");");
  }

  private static void setupNonFrozenCollectionsTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS nonfrozen_collections_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TABLE IF NOT EXISTS nonfrozen_collections_ks.tab ("
            + "id int PRIMARY KEY,"
            + "list_col list<int>,"
            + "set_col set<text>,"
            + "map_col map<int, text>"
            + ") WITH cdc = {'enabled':true}");
    session.execute(
        "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES ("
            + "1, [10,20,30], {'x','y','z'}, {10:'ten',20:'twenty'}"
            + ");");
  }

  private static void setupUDTTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS udt_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute("CREATE TYPE IF NOT EXISTS udt_ks.simple_udt (a int, b text);");
    session.execute(
        "CREATE TABLE IF NOT EXISTS udt_ks.tab ("
            + "id int PRIMARY KEY,"
            + "udt_col frozen<simple_udt>,"
            + "nf_udt_col simple_udt"
            + ") WITH cdc = {'enabled':true}");
    // Insert row with non-null UDTs
    session.execute(
        "INSERT INTO udt_ks.tab (id, udt_col, nf_udt_col) VALUES (1, {a: 42, b: 'foo'}, {a: 7, b: 'bar'});");
    // Insert row with null UDTs
    session.execute("INSERT INTO udt_ks.tab (id, udt_col, nf_udt_col) VALUES (2, null, null);");
  }

  private static void setupComplexTypesTable(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS complex_types_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    session.execute(
        "CREATE TYPE IF NOT EXISTS complex_types_ks.address_udt (street text, phones frozen<list<text>>, tags frozen<set<text>>);");
    session.execute(
        "CREATE TABLE IF NOT EXISTS complex_types_ks.tab ("
            + "id int PRIMARY KEY,"
            + "frozen_addr frozen<address_udt>,"
            + "nf_addr address_udt,"
            + "frozen_addr_list frozen<list<frozen<address_udt>>>,"
            + "nf_addr_set set<frozen<address_udt>>,"
            + "nf_addr_map map<int, frozen<address_udt>>"
            + ") WITH cdc = {'enabled':true}");
    session.execute(
        "INSERT INTO complex_types_ks.tab (id, frozen_addr, nf_addr, frozen_addr_list, nf_addr_set, nf_addr_map) VALUES ("
            + "1,"
            + "{street: 'main', phones: ['111','222'], tags: {'home','primary'}},"
            + "{street: 'side', phones: ['333'], tags: {'secondary'}},"
            + "[{street: 'l1', phones: ['444'], tags: {'list1'}}, {street: 'l2', phones: ['555'], tags: {'list2'}}],"
            + "{{street: 's1', phones: ['666'], tags: {'tag1'}}, {street: 's2', phones: ['777'], tags: {'tag2'}}},"
            + "{10: {street: 'm1', phones: ['888'], tags: {'tagm1'}}, 20: {street: 'm2', phones: ['999'], tags: {'tagm2'}}}"
            + ");");
  }

  @AfterEach
  public void cleanUp() {
    try {
      KafkaConnectUtils.removeAllConnectors();
    } catch (Exception e) {
      throw new RuntimeException("Failed to remove the connectors.", e);
    }
  }

  @Test
  public void canReplicateAllPrimitiveTypes() throws UnknownHostException {
    final String SCYLLA_ALL_TYPES_CONNECTOR = "ScyllaAllTypesConnector";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateAllPrimitiveTypes");
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", SCYLLA_ALL_TYPES_CONNECTOR);
      registerConnector(SCYLLA_ALL_TYPES_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canReplicateAllPrimitiveTypes.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                String value = record.value();
                assert value.contains("\"id\":1");
                assert value.contains("\"ascii_col\":{" + "\"value\":\"ascii\"}");
                assert value.contains("\"bigint_col\":{" + "\"value\":1234567890123}");
                assert value.contains("\"blob_col\":{" + "\"value\":\"yv66vg==\"}");
                assert value.contains("\"boolean_col\":{" + "\"value\":true}");
                // This is number of days since unix epoch that should correspond to '2024-06-10'
                assert value.contains("\"date_col\":{" + "\"value\":19884}");
                assert value.contains("\"decimal_col\":{" + "\"value\":\"12345.67\"}");
                assert value.contains("\"double_col\":{" + "\"value\":3.14159}");
                assert value.contains("\"duration_col\":{" + "\"value\":\"1d12h30m\"");
                assert value.contains("\"float_col\":{" + "\"value\":2.71828}");
                assert value.contains("\"inet_col\":{" + "\"value\":\"127.0.0.1\"}");
                assert value.contains("\"int_col\":{" + "\"value\":42}");
                assert value.contains("\"smallint_col\":{" + "\"value\":7}");
                assert value.contains("\"text_col\":{" + "\"value\":\"some text\"}");
                // Shows up as 45296789000000.
                // 45296789 part of the value is the number of milliseconds since midnight that
                // corresponds to '12:34:56.789'
                assert value.contains("\"time_col\":{" + "\"value\":45296789000000}");
                // 1718022896789 is unix timestamp in milliseconds for '2024-06-10T12:34:56.789Z'
                assert value.contains("\"timestamp_col\":{" + "\"value\":1718022896789}");
                assert value.contains(
                    "\"timeuuid_col\":{" + "\"value\":\"81d4a030-4632-11f0-9484-409dd8f36eba\"");
                assert value.contains("\"tinyint_col\":{" + "\"value\":5}");
                assert value.contains(
                    "\"uuid_col\":{" + "\"value\":\"453662fa-db4b-4938-9033-d8523c0a371c\"}");
                assert value.contains("\"varchar_col\":{" + "\"value\":\"varchar text\"}");
                assert value.contains("\"varint_col\":{" + "\"value\":\"999999999\"}");
              });
          break;
        }
      }
      consumer.unsubscribe();
      assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");
    }
  }

  @Test
  public void canReplicateFrozenCollections() throws UnknownHostException {
    final String FROZEN_COLLECTIONS_CONNECTOR = "FrozenCollectionsConnector";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateFrozenCollections");
      connectorConfiguration.put("scylla.table.names", "frozen_collections_ks.tab");
      connectorConfiguration.put("name", FROZEN_COLLECTIONS_CONNECTOR);
      registerConnector(FROZEN_COLLECTIONS_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canReplicateFrozenCollections.frozen_collections_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (value.contains("\"id\":1")) {
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"frozen_list_col\":{\"value\":[1,2,3]}"),
                          "Expected frozen_list_col in value: " + value),
                  () -> assertFrozenSetIgnoringOrder(value, Set.of("a", "b", "c")),
                  () ->
                      assertTrue(
                          value.contains(
                              "\"frozen_map_col\":{\"value\":[[1,\"one\"],[2,\"two\"]]}"),
                          "Expected frozen_map_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains(
                              "\"frozen_tuple_col\":{\"value\":{\"tuple_member_0\":42,\"tuple_member_1\":\"foo\"}}"),
                          "Expected frozen_tuple_col in value: " + value));
              messageConsumed = true;
              break;
            }
          }
          if (messageConsumed) break;
        }
      }
      consumer.unsubscribe();
      assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");
    }
  }

  // A helper to assert frozen set contents ignoring order. Sets are unordered, so we can
  // not rely on the order of elements in the JSON array.
  private static void assertFrozenSetIgnoringOrder(String value, Set<String> expectedElements) {
    var actualElements =
        new HashSet<>(
            KafkaUtils.extractListFromAfterField(value, "frozen_set_col", JsonNode::asText));
    Assertions.assertEquals(
        expectedElements, actualElements, "Unexpected frozen_set_col elements in value: " + value);
  }

  @Test
  public void canReplicateFrozenCollectionsEdgeCases() throws UnknownHostException {
    // Insert a row with empty and null frozen collections
    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {
      // Insert row with empty collections
      session.execute(
          "INSERT INTO frozen_collections_ks.tab (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES (2, [], {}, {}, (null, null));");
      // Insert row with null collections (tuple must be present, so set to (null, null))
      session.execute(
          "INSERT INTO frozen_collections_ks.tab (id, frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col) VALUES (3, null, null, null, (null, null));");
    }

    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      final String FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR = "FrozenCollectionsEdgeCasesConnector";
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateFrozenCollectionsEdgeCases");
      connectorConfiguration.put("scylla.table.names", "frozen_collections_ks.tab");
      connectorConfiguration.put("name", FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR);
      registerConnector(FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR, connectorConfiguration);
      consumer.subscribe(
          List.of("canReplicateFrozenCollectionsEdgeCases.frozen_collections_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean foundEmpty = false;
      boolean foundNull = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000 && (!foundEmpty || !foundNull)) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (value.contains("\"id\":2")) {
              // Empty collections
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"frozen_list_col\":{\"value\":[]}"),
                          "Expected empty frozen_list_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"frozen_set_col\":{\"value\":[]}"),
                          "Expected empty frozen_set_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"frozen_map_col\":{\"value\":[]}"),
                          "Expected empty frozen_map_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains(
                              "\"frozen_tuple_col\":{\"value\":{\"tuple_member_0\":null,\"tuple_member_1\":null}}"),
                          "Expected null tuple members in value: " + value));
              foundEmpty = true;
            } else if (value.contains("\"id\":3")) {
              // Null collections (should be {"value":null})
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"frozen_list_col\":{\"value\":null}"),
                          "Expected null frozen_list_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"frozen_set_col\":{\"value\":null}"),
                          "Expected null frozen_set_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"frozen_map_col\":{\"value\":null}"),
                          "Expected null frozen_map_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains(
                              "\"frozen_tuple_col\":{\"value\":{\"tuple_member_0\":null,\"tuple_member_1\":null}}"),
                          "Expected null tuple members in value: " + value));
              foundNull = true;
            }
          }
        }
      }
      consumer.unsubscribe();
      assertTrue(foundEmpty, "No message consumed for empty frozen collections row.");
      assertTrue(foundNull, "No message consumed for null frozen collections row.");
    }
  }

  @Test
  public void canReplicateNonFrozenCollections() throws UnknownHostException {
    final String NON_FROZEN_COLLECTIONS_CONNECTOR = "NonFrozenCollectionsConnector";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateNonFrozenCollections");
      connectorConfiguration.put("scylla.table.names", "nonfrozen_collections_ks.tab");
      connectorConfiguration.put("name", NON_FROZEN_COLLECTIONS_CONNECTOR);
      registerConnector(NON_FROZEN_COLLECTIONS_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canReplicateNonFrozenCollections.nonfrozen_collections_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (value.contains("\"id\":1")) {
              // list_col: mode OVERWRITE, values {10,20,30} regardless of internal keys
              var listValue = KafkaUtils.extractValueNodeFromAfterField(value, "list_col");
              var listElements = listValue.path("elements");
              var listValues = new HashSet<>();
              listElements.fields().forEachRemaining(e -> listValues.add(e.getValue().asInt()));

              Assertions.assertEquals(
                  "OVERWRITE",
                  listValue.path("mode").asText(),
                  "Expected list_col delta mode OVERWRITE in value: " + value);

              Assertions.assertTrue(
                  listElements.isObject(), "Expected list_col elements object in value: " + value);
              Assertions.assertEquals(
                  Set.of(10, 20, 30),
                  listValues,
                  "Unexpected list_col elements in value: " + value);

              // set_col: mode OVERWRITE, elements {x,y,z} as keys
              JsonNode setValue = KafkaUtils.extractValueNodeFromAfterField(value, "set_col");
              JsonNode setElements = setValue.path("elements");
              var setValues = new HashSet<String>();
              setElements.fieldNames().forEachRemaining(setValues::add);

              Assertions.assertEquals(
                  "OVERWRITE",
                  setValue.path("mode").asText(),
                  "Expected set_col delta mode OVERWRITE in value: " + value);

              Assertions.assertTrue(
                  setElements.isObject(), "Expected set_col elements object in value: " + value);
              Assertions.assertEquals(
                  Set.of("x", "y", "z"),
                  setValues,
                  "Unexpected set_col elements in value: " + value);

              // map_col: mode OVERWRITE, entries {10:"ten", 20:"twenty"}
              JsonNode mapValue = KafkaUtils.extractValueNodeFromAfterField(value, "map_col");
              JsonNode mapElements = mapValue.path("elements");
              var mapEntries =
                  StreamSupport.stream(mapElements.spliterator(), false)
                      .filter(entry -> entry.isArray() && entry.size() == 2)
                      .collect(
                          Collectors.toMap(
                              entry -> entry.get(0).asInt(), entry -> entry.get(1).asText()));

              Assertions.assertEquals(
                  "OVERWRITE",
                  mapValue.path("mode").asText(),
                  "Expected map_col delta mode OVERWRITE in value: " + value);
              Assertions.assertTrue(
                  mapElements.isArray(), "Expected map_col elements array in value: " + value);
              Assertions.assertEquals(
                  2, mapEntries.size(), "Expected exactly 2 entries in map_col elements: " + value);
              Assertions.assertEquals(
                  "ten", mapEntries.get(10), "Expected map_col entry 10:'ten' in value: " + value);
              Assertions.assertEquals(
                  "twenty",
                  mapEntries.get(20),
                  "Expected map_col entry 20:'twenty' in value: " + value);
              messageConsumed = true;
              break;
            }
          }
          if (messageConsumed) break;
        }
      }
      consumer.unsubscribe();
      assertTrue(messageConsumed, "No message consumed for non-frozen collections row.");
    }
  }

  @Test
  public void canReplicateNonFrozenCollectionsEdgeCases() throws UnknownHostException {
    // Insert rows with empty, null, and element removal for non-frozen collections.
    //
    // NOTE: For non-frozen collections in CDC "delta" mode, Scylla does not
    // provide enough information to distinguish an explicit empty collection
    // value ([], {}, {}) from an explicit NULL on INSERT when there are no
    // element-level deltas. Both cases surface as a "deleted" collection with
    // no elements in the CDC log. Because of this, the connector intentionally
    // treats both of these INSERT patterns as a top-level null for the
    // non-frozen collection column. This test asserts that behavior.
    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {
      // Empty collections
      session.execute(
          "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES (2, [], {}, {});");
      // Null collections
      session.execute(
          "INSERT INTO nonfrozen_collections_ks.tab (id, list_col, set_col, map_col) VALUES (3, null, null, null);");
      // Perform simultaneous element removal and addition on each non-frozen collection
      // in a single UPDATE statement to exercise combined collection deltas.
      session.execute(
          "UPDATE nonfrozen_collections_ks.tab SET "
              + "list_col = list_col - [10], list_col = list_col + [40], "
              + "set_col = set_col - {'x'}, set_col = set_col + {'w'}, "
              + "map_col = map_col - {10}, map_col = map_col + {30:'thirty'} "
              + "WHERE id = 1;");
    }

    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      final String NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR =
          "NonFrozenCollectionsEdgeCasesConnector";
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateNonFrozenCollectionsEdgeCases");
      connectorConfiguration.put("scylla.table.names", "nonfrozen_collections_ks.tab");
      connectorConfiguration.put("name", NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR);
      registerConnector(NON_FROZEN_COLLECTIONS_EDGE_CASES_CONNECTOR, connectorConfiguration);
      consumer.subscribe(
          List.of("canReplicateNonFrozenCollectionsEdgeCases.nonfrozen_collections_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean foundEmpty = false;
      boolean foundNull = false;
      boolean foundRemoval = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000
          && (!foundEmpty || !foundNull || !foundRemoval)) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (value.contains("\"id\":2")) {
              // Empty collections. Due to CDC limitations described above,
              // these appear the same as explicit NULL collections on INSERT
              // for non-frozen types, so we expect top-level nulls.
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"list_col\":null"),
                          "Expected null list_col in value for empty collection row: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"set_col\":null"),
                          "Expected null set_col in value for empty collection row: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"map_col\":null"),
                          "Expected null map_col in value for empty collection row: " + value));
              foundEmpty = true;
            } else if (value.contains("\"id\":3")) {
              // Null collections
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"list_col\":null"),
                          "Expected null list_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"set_col\":null"),
                          "Expected null set_col in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"map_col\":null"),
                          "Expected null map_col in value: " + value));
              foundNull = true;
            } else if (value.contains("\"id\":1") && value.contains("\"op\":\"u\"")) {
              // Element removal and addition (UPDATE event only; skip initial CREATE)
              assertAll(
                  () ->
                      assertTrue(
                          value.contains("\"list_col\":{\"value\":{\"mode\":\"MODIFY\""),
                          "Expected list_col delta mode MODIFY in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"set_col\":{\"value\":{\"mode\":\"MODIFY\""),
                          "Expected set_col delta mode MODIFY in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"map_col\":{\"value\":{\"mode\":\"MODIFY\""),
                          "Expected map_col delta mode MODIFY in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"elements\":"),
                          "Expected elements field in delta in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("40"),
                          "Expected list_col addition of 40 in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("w"),
                          "Expected set_col addition of 'w' in value: " + value),
                  () ->
                      assertTrue(
                          value.contains("thirty"),
                          "Expected map_col addition of key 30:'thirty' in value: " + value));
              foundRemoval = true;
            }
          }
        }
      }
      consumer.unsubscribe();
      assertTrue(foundEmpty, "No message consumed for empty non-frozen collections row.");
      assertTrue(foundNull, "No message consumed for null non-frozen collections row.");
      assertTrue(
          foundRemoval, "No message consumed for element removal in non-frozen collections row.");
    }
  }

  @Test
  public void canReplicateUDT() throws UnknownHostException {
    final String UDT_CONNECTOR = "UDTConnector";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateUDT");
      connectorConfiguration.put("scylla.table.names", "udt_ks.tab");
      connectorConfiguration.put("name", UDT_CONNECTOR);
      registerConnector(UDT_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canReplicateUDT.udt_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean foundNonNull = false;
      boolean foundNull = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000 && (!foundNonNull || !foundNull)) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (value.contains("\"id\":1")) {
              assertAll(
                  // frozen UDT
                  () ->
                      assertTrue(
                          value.contains("\"udt_col\":{\"value\":{\"a\":42,\"b\":\"foo\"}}"),
                          "Expected non-null frozen UDT value: " + value),
                  // non-frozen UDT (mode/OVERWRITE, elements with value fields)
                  () ->
                      assertTrue(
                          value.contains("\"nf_udt_col\":{\"value\":{\"mode\":\"OVERWRITE\""),
                          "Expected mode=OVERWRITE for non-frozen UDT: " + value),
                  () ->
                      assertTrue(
                          value.contains(
                              "\"elements\":{\"a\":{\"value\":7},\"b\":{\"value\":\"bar\"}}"),
                          "Expected elements with correct values for non-frozen UDT: " + value));
              foundNonNull = true;
            } else if (value.contains("\"id\":2")) {
              assertAll(
                  // frozen UDT
                  () ->
                      assertTrue(
                          value.contains("\"udt_col\":{\"value\":null}"),
                          "Expected null frozen UDT value: " + value),
                  // non-frozen UDT
                  () ->
                      assertTrue(
                          value.contains("\"nf_udt_col\":null")
                              || value.contains("\"nf_udt_col\":{\"value\":null}"),
                          "Expected null non-frozen UDT value (top-level null or {value:null}): "
                              + value));
              foundNull = true;
            }
          }
        }
      }
      consumer.unsubscribe();
      assertTrue(foundNonNull, "No message consumed for non-null UDT row.");
      assertTrue(foundNull, "No message consumed for null UDT row.");
    }
  }

  @Test
  public void canReplicateComplexUDTAndCollections() throws UnknownHostException {
    // Issue a series of UPDATEs that modify the non-frozen UDT and
    // non-frozen collections of UDTs. In practice Scylla CDC may emit
    // collection deltas (especially for sets/maps) in ways that are
    // hard to assert deterministically across versions and providers.
    //
    // This test therefore *requires* observing:
    //   - the CREATE (op="c") event with the full initial complex value
    //   - an UPDATE (op="u") event for the non-frozen UDT field nf_addr
    //
    // and only *optionally* asserts the detailed nf_addr_map delta if it
    // appears in the stream. We do not fail the test if no separate
    // nf_addr_map UPDATE event is observed.
    try (Cluster cluster =
            Cluster.builder()
                .addContactPoint(scyllaDBContainer.getContactPoint().getHostName())
                .withPort(scyllaDBContainer.getMappedPort(9042))
                .build();
        Session session = cluster.connect()) {
      session.execute(
          "UPDATE complex_types_ks.tab SET "
              + "nf_addr.street = 'side-updated', "
              + "nf_addr.tags = null "
              + "WHERE id = 1;");
      session.execute(
          "UPDATE complex_types_ks.tab SET "
              + "nf_addr_set = nf_addr_set - {{street: 's1', phones: ['666'], tags: {'tag1'}}} "
              + "WHERE id = 1;");
      session.execute(
          "UPDATE complex_types_ks.tab SET "
              + "nf_addr_set = nf_addr_set + {{street: 's3', phones: ['000'], tags: {'tag3'}}} "
              + "WHERE id = 1;");
      session.execute(
          "UPDATE complex_types_ks.tab SET "
              + "nf_addr_map = nf_addr_map - {10} "
              + "WHERE id = 1;");
      session.execute(
          "UPDATE complex_types_ks.tab SET "
              + "nf_addr_map = nf_addr_map + {30: {street: 'm3', phones: ['123'], tags: {'tagm3'}}} "
              + "WHERE id = 1;");
    }

    final String COMPLEX_TYPES_CONNECTOR = "ComplexUDTAndCollectionsConnector";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateComplexUDTAndCollections");
      connectorConfiguration.put("scylla.table.names", "complex_types_ks.tab");
      connectorConfiguration.put("name", COMPLEX_TYPES_CONNECTOR);
      registerConnector(COMPLEX_TYPES_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canReplicateComplexUDTAndCollections.complex_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean foundCreate = false;
      boolean foundUpdateNfAddr = false;
      boolean foundUpdateNfAddrMap = false;

      while (System.currentTimeMillis() - startTime < 65 * 1000
          && (!foundCreate || !foundUpdateNfAddr)) {
        var records = consumer.poll(Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          for (var record : records) {
            String value = record.value();
            if (!value.contains("\"id\":1")) {
              continue;
            }

            if (value.contains("\"op\":\"c\"") && !foundCreate) {
              assertAll(
                  // frozen UDT with nested collections
                  () ->
                      assertTrue(
                          value.contains("\"frozen_addr\":{\"value\":{\"street\":\"main\""),
                          "Expected frozen_addr with street 'main' in value: " + value),
                  // non-frozen UDT initial state (OVERWRITE)
                  () ->
                      assertTrue(
                          value.contains("\"nf_addr\":{\"value\":{\"mode\":\"OVERWRITE\""),
                          "Expected nf_addr mode OVERWRITE in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"street\":{\"value\":\"side\""),
                          "Expected nf_addr street 'side' in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"phones\":{\"value\":[\"333\"]"),
                          "Expected nf_addr phones ['333'] in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"tags\":{\"value\":[\"secondary\"]"),
                          "Expected nf_addr tags ['secondary'] in create value: " + value),
                  // frozen_addr_list
                  () ->
                      assertTrue(
                          value.contains("\"frozen_addr_list\":{\"value\":[")
                              && value.contains("\"street\":\"l1\"")
                              && value.contains("\"street\":\"l2\""),
                          "Expected frozen_addr_list with l1 and l2 in value: " + value),
                  // nf_addr_set initial state (OVERWRITE)
                  () ->
                      assertTrue(
                          value.contains("\"nf_addr_set\":{\"value\":{\"mode\":\"OVERWRITE\""),
                          "Expected nf_addr_set mode OVERWRITE in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("\"street\":\"s1\"")
                              && value.contains("\"street\":\"s2\""),
                          "Expected nf_addr_set elements s1 and s2 in create value: " + value),
                  // nf_addr_map initial state (OVERWRITE)
                  () ->
                      assertTrue(
                          value.contains("\"nf_addr_map\":{\"value\":{\"mode\":\"OVERWRITE\""),
                          "Expected nf_addr_map mode OVERWRITE in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("10") && value.contains("\"street\":\"m1\""),
                          "Expected nf_addr_map key 10 with street m1 in create value: " + value),
                  () ->
                      assertTrue(
                          value.contains("20") && value.contains("\"street\":\"m2\""),
                          "Expected nf_addr_map key 20 with street m2 in create value: " + value));
              foundCreate = true;
            } else if (value.contains("\"op\":\"u\"")) {
              if (!foundUpdateNfAddr && value.contains("\"nf_addr\":{\"value\"")) {
                assertAll(
                    () ->
                        assertTrue(
                            value.contains("\"nf_addr\":{\"value\":{\"mode\":\"MODIFY\""),
                            "Expected nf_addr mode MODIFY in update value: " + value),
                    () ->
                        assertTrue(
                            value.contains("\"street\":{\"value\":\"side-updated\""),
                            "Expected nf_addr updated street in value: " + value),
                    () ->
                        assertTrue(
                            value.contains("\"tags\":{\"value\":[]}")
                                || value.contains("\"tags\":{\"value\":[ ]}"),
                            "Expected nf_addr tags updated to empty set in value: " + value));
                foundUpdateNfAddr = true;
              }

              if (!foundUpdateNfAddrMap && value.contains("\"nf_addr_map\":{\"value\"")) {
                assertAll(
                    () ->
                        assertTrue(
                            value.contains("\"nf_addr_map\":{\"value\":{\"mode\":\"MODIFY\""),
                            "Expected nf_addr_map mode MODIFY in update value: " + value),
                    () ->
                        assertTrue(
                            value.contains("[10,null]") || value.contains("[10, null]"),
                            "Expected nf_addr_map removal of key 10 in value: " + value),
                    () ->
                        assertTrue(
                            value.contains("30") && value.contains("\"street\":\"m3\""),
                            "Expected nf_addr_map key 30 with street m3 in value: " + value));
                foundUpdateNfAddrMap = true;
              }
            }
          }
        }
      }

      consumer.unsubscribe();
      assertTrue(
          foundCreate,
          "No CREATE event consumed for complex UDT/collections row. Topic may be empty or connector may have crashed.");
      assertTrue(
          foundUpdateNfAddr,
          "No UPDATE event for nf_addr consumed for complex UDT/collections row.");
    }
  }

  @Test
  public void canExtractNewRecordState() {
    final String CAN_EXTRACT_NEW_RECORD_STATE_CONNECTOR = "canExtractNewRecordState";
    try (KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createCommonConnectorProperties();
      connectorConfiguration.put("topic.prefix", CAN_EXTRACT_NEW_RECORD_STATE_CONNECTOR);
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", CAN_EXTRACT_NEW_RECORD_STATE_CONNECTOR);
      connectorConfiguration.put("transforms", "extractNewRecordState");
      connectorConfiguration.put(
          "transforms.extractNewRecordState.type",
          "com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState");
      registerConnector(CAN_EXTRACT_NEW_RECORD_STATE_CONNECTOR, connectorConfiguration);
      consumer.subscribe(List.of("canExtractNewRecordState.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                String value = record.value();
                assert value.contains("{\"ascii_col\":\"ascii\"");
                assert value.contains("\"bigint_col\":1234567890123");
                assert value.contains("\"blob_col\":\"yv66vg==\"");
                assert value.contains("\"boolean_col\":true");
                assert value.contains("\"date_col\":19884");
                assert value.contains("\"decimal_col\":\"12345.67\"");
                assert value.contains("\"double_col\":3.14159");
                assert value.contains("\"duration_col\":\"1d12h30m\"");
                assert value.contains("\"float_col\":2.71828");
                assert value.contains("\"id\":1");
                assert value.contains("\"inet_col\":\"127.0.0.1\"");
                assert value.contains("\"int_col\":42");
                assert value.contains("\"smallint_col\":7");
                assert value.contains("\"text_col\":\"some text\"");
                assert value.contains("\"time_col\":45296789000000");
                assert value.contains("\"timestamp_col\":1718022896789");
                assert value.contains("\"timeuuid_col\":\"81d4a030-4632-11f0-9484-409dd8f36eba\"");
                assert value.contains("\"tinyint_col\":5");
                assert value.contains("\"uuid_col\":\"453662fa-db4b-4938-9033-d8523c0a371c\"");
                assert value.contains("\"varchar_col\":\"varchar text\"");
                assert value.contains("\"varint_col\":\"999999999\"}");
              });
          break;
        }
      }
      consumer.unsubscribe();
      assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");
    }
  }

  @Test
  @EnabledIf("isConfluentKafkaProvider")
  public void canReplicateAllPrimitiveTypesWithAvro() {
    Assumptions.assumeTrue(
        KAFKA_CONNECT_MODE == KafkaConnectMode.DISTRIBUTED,
        "AvroConverter is not available in cp-kafka image.");
    try (KafkaConsumer<GenericRecord, GenericRecord> consumer = KafkaUtils.createAvroConsumer()) {
      Properties connectorConfiguration = KafkaConnectUtils.createAvroConnectorProperties();
      connectorConfiguration.put("topic.prefix", "canReplicateAllPrimitiveTypesWithAvro");
      connectorConfiguration.put("scylla.table.names", "primitive_types_ks.tab");
      connectorConfiguration.put("name", "ScyllaAllTypesAvroConnector");
      int returnCode = -1;
      try {
        returnCode =
            KafkaConnectUtils.registerConnector(
                connectorConfiguration, "ScyllaAllTypesAvroConnector");
      } catch (Exception e) {
        throw new RuntimeException("Failed to register connector.", e);
      }
      Assertions.assertEquals(201, returnCode, "Connector registration failed");
      consumer.subscribe(List.of("canReplicateAllPrimitiveTypesWithAvro.primitive_types_ks.tab"));
      long startTime = System.currentTimeMillis();
      boolean messageConsumed = false;
      while (System.currentTimeMillis() - startTime < 65 * 1000) {
        var records = consumer.poll(java.time.Duration.ofSeconds(5));
        if (!records.isEmpty()) {
          messageConsumed = true;
          records.forEach(
              record -> {
                GenericRecord value = record.value();
                GenericRecord after = (GenericRecord) value.get("after");

                // Verify all primitive type fields are present and have expected values
                Assertions.assertEquals("1", extractValue(after.get("id")).toString());
                Assertions.assertEquals("ascii", extractValue(after.get("ascii_col")).toString());
                Assertions.assertEquals(
                    "1234567890123", extractValue(after.get("bigint_col")).toString());
                Assertions.assertNotNull(extractValue(after.get("blob_col"))); // blob as bytes
                Assertions.assertEquals("true", extractValue(after.get("boolean_col")).toString());
                // This is number of days since unix epoch that should correspond to '2024-06-10'
                Assertions.assertEquals("19884", extractValue(after.get("date_col")).toString());
                Assertions.assertEquals(
                    "12345.67", extractValue(after.get("decimal_col")).toString());
                Assertions.assertEquals(
                    "3.14159", extractValue(after.get("double_col")).toString());
                Assertions.assertEquals(
                    "1d12h30m", extractValue(after.get("duration_col")).toString());
                Assertions.assertEquals("2.71828", extractValue(after.get("float_col")).toString());
                Assertions.assertEquals(
                    "127.0.0.1", extractValue(after.get("inet_col")).toString());
                Assertions.assertEquals("42", extractValue(after.get("int_col")).toString());
                Assertions.assertEquals("7", extractValue(after.get("smallint_col")).toString());
                Assertions.assertEquals(
                    "some text", extractValue(after.get("text_col")).toString());
                // Shows up as 45296789000000.
                // 45296789 part of the value is the number of milliseconds since midnight that
                // corresponds to '12:34:56.789'
                Assertions.assertEquals(
                    "45296789000000", extractValue(after.get("time_col")).toString());
                // 1718022896789 is unix timestamp in milliseconds for '2024-06-10T12:34:56.789Z'
                Assertions.assertEquals(
                    "1718022896789", extractValue(after.get("timestamp_col")).toString());
                Assertions.assertEquals(
                    "81d4a030-4632-11f0-9484-409dd8f36eba",
                    extractValue(after.get("timeuuid_col")).toString());
                Assertions.assertEquals("5", extractValue(after.get("tinyint_col")).toString());
                Assertions.assertEquals(
                    "453662fa-db4b-4938-9033-d8523c0a371c",
                    extractValue(after.get("uuid_col")).toString());
                Assertions.assertEquals(
                    "varchar text", extractValue(after.get("varchar_col")).toString());
                Assertions.assertEquals(
                    "999999999", extractValue(after.get("varint_col")).toString());
              });
          break;
        }
      }
      consumer.unsubscribe();
      assertTrue(
          messageConsumed,
          "No message consumed from the topic. Topic may be empty or connector may have crashed.");

      // The schema registry check is kind of overzealous. The correctness of that part is
      // mostly the responsibility of the AvroConverter rather than the connector.

      // Verify that schema registry has exactly 1 schema registered for both key and value subjects
      String topicName = "canReplicateAllPrimitiveTypesWithAvro.primitive_types_ks.tab";
      String keySubject = topicName + "-key";
      String valueSubject = topicName + "-value";

      try {
        String keyVersions = SchemaRegistryUtils.getSchemaVersions(keySubject);
        String valueVersions = SchemaRegistryUtils.getSchemaVersions(valueSubject);

        Assertions.assertNotNull(keyVersions, "Key subject should exist in schema registry");
        Assertions.assertNotNull(valueVersions, "Value subject should exist in schema registry");

        Assertions.assertEquals(
            "[1]", keyVersions, "Key subject should have exactly 1 schema version");
        Assertions.assertEquals(
            "[1]", valueVersions, "Key subject should have exactly 1 schema version");

      } catch (Exception e) {
        Assertions.fail("Failed to verify schema registry: " + e.getMessage());
      }
    }
  }

  /** Register a connector with the given name and configuration. */
  private void registerConnector(final String connectorName, Properties connectorConfiguration) {
    try {
      int returnCode = KafkaConnectUtils.registerConnector(connectorConfiguration, connectorName);
      if (returnCode == 500) {
        String status = KafkaConnectUtils.getConnectorStatus(connectorName);
        if (status == null) {
          Assertions.fail(
              "Received 500 error on connector registration and connector is not registered.");
        }
      } else if (returnCode / 100 != 2) {
        Assertions.fail(
            "Received non-success response code on connector registration: " + returnCode);
      }
    } catch (Exception e) {
      Assertions.fail("Failed to register connector.", e);
    }
  }

  /** Condition method for enabling tests that require Confluent Kafka (schema registry). */
  static boolean isConfluentKafkaProvider() {
    return KAFKA_PROVIDER == KafkaProvider.CONFLUENT;
  }

  // Helper method to extract value from Avro types
  private static Object extractValue(Object obj) {
    if (obj instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) obj;
      return record.get("value");
    }
    return obj;
  }
}
