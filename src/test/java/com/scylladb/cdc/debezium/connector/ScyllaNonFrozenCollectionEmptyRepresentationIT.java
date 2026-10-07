package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

/** Tests configurable empty representation for ambiguous non-frozen collection values. */
public class ScyllaNonFrozenCollectionEmptyRepresentationIT extends ScyllaTypesIT<String, String> {

  @Override
  protected String createTableCql(String tableName) {
    return "(id int PRIMARY KEY, list_col list<int>, set_col set<text>, map_col map<int, text>, "
        + "frozen_list_col frozen<list<int>>, frozen_set_col frozen<set<text>>, "
        + "frozen_map_col frozen<map<int, text>>)";
  }

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    Properties props = new Properties();
    props.put(ScyllaConnectorConfig.CDC_NON_FROZEN_COLLECTION_EMPTY_REPRESENTATION_KEY, "empty");
    props.put("cdc.include.after", "only-updated");
    return KafkaConnectUtils.buildPlainConnector(connectorName, tableName, props);
  }

  @Override
  protected int extractPkFromValue(String value) {
    int pk = extractIdFromKeyField(value);
    if (pk != -1) {
      return pk;
    }
    return extractIdFromJson(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  @Test
  void insertEmptyNonFrozenCollectionsCanEmitEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, [], {}, {})"
            .formatted(getSuiteKeyspaceTableName(), pk));

    waitAndAssert(
        pk, new String[] {expectedRecord("c", "null", afterEmpty(pk), expectedKeyFor(pk))});
  }

  @Test
  void insertNullNonFrozenCollectionsCanEmitEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, null, null, null)"
            .formatted(getSuiteKeyspaceTableName(), pk));

    waitAndAssert(
        pk, new String[] {expectedRecord("c", "null", afterEmpty(pk), expectedKeyFor(pk))});
  }

  @Test
  void updateFromValueToEmptyNonFrozenCollectionsCanEmitEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, [10], {'x'}, {10: 'ten'})"
            .formatted(getSuiteKeyspaceTableName(), pk));
    session.execute(
        "UPDATE %s SET list_col = [], set_col = {}, map_col = {} WHERE id = %d"
            .formatted(getSuiteKeyspaceTableName(), pk));

    waitAndAssert(
        pk,
        new String[] {
          expectedRecord("c", "null", afterWithValues(pk), expectedKeyFor(pk)),
          expectedRecord("u", afterWithValues(pk), afterEmpty(pk), expectedKeyFor(pk))
        });
  }

  @Test
  void onlyUpdatedAfterIncludesNullNonFrozenCollectionAsEmpty() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO %s (id, list_col, set_col, map_col) VALUES (%d, [10], {'x'}, {10: 'ten'})"
            .formatted(getSuiteKeyspaceTableName(), pk));
    session.execute(
        "UPDATE %s SET list_col = null WHERE id = %d".formatted(getSuiteKeyspaceTableName(), pk));

    waitAndAssert(
        pk,
        new String[] {
          expectedRecord("c", "null", afterWithValues(pk), expectedKeyFor(pk)),
          expectedRecord(
              "u",
              afterWithValues(pk),
              afterCollections(pk, "[]", "null", "null"),
              expectedKeyFor(pk))
        });
  }

  private String expectedKeyFor(int pk) {
    return "{\"id\": %d}".formatted(pk);
  }

  private String afterEmpty(int pk) {
    return afterCollections(pk, "[]", "[]", "[]");
  }

  private String afterWithValues(int pk) {
    return afterCollections(pk, "[10]", "[\"x\"]", "[{\"key\": 10, \"value\": \"ten\"}]");
  }

  private String afterCollections(int pk, String listValue, String setValue, String mapValue) {
    return """
        {
          "id": %d,
          "list_col": %s,
          "set_col": %s,
          "map_col": %s,
          "frozen_list_col": null,
          "frozen_set_col": null,
          "frozen_map_col": null
        }
        """
        .formatted(pk, listValue, setValue, mapValue);
  }
}
