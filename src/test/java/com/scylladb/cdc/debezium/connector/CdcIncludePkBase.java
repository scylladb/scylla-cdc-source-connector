package com.scylladb.cdc.debezium.connector;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

/**
 * Base integration tests for cdc.include.primary-key.placement configuration options.
 *
 * <p>Tests verify that primary key columns are correctly included or excluded in the Kafka record
 * key and value payload based on the cdc.include.primary-key.placement configuration.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class CdcIncludePkBase<K, V> extends ScyllaTypesIT<K, V> {

  /**
   * Returns the expected JSON for an INSERT operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.primary-key.placement
   * configuration.
   */
  abstract String[] expectedInsert(int pk);

  /**
   * Returns the expected JSON for a DELETE operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.primary-key.placement
   * configuration.
   */
  abstract String[] expectedDelete(int pk);

  /**
   * Returns the expected JSON for an UPDATE operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.primary-key.placement
   * configuration.
   */
  abstract String[] expectedUpdate(int pk);

  @Override
  protected String createTableCql(String tableName) {
    return "(id int PRIMARY KEY, name text)";
  }

  /**
   * Builds a KafkaConsumer with the specified primary key placement configuration.
   *
   * <p>This helper method encapsulates the common connector setup pattern: creating properties,
   * setting CDC configuration, registering the connector, and subscribing to the topic.
   *
   * @param connectorName the name of the connector
   * @param tableName the table name to subscribe to
   * @param pkPlacement the cdc.include.primary-key.placement configuration value
   * @return a configured KafkaConsumer
   */
  protected static KafkaConsumer<String, String> buildStringConsumer(
      String connectorName, String tableName, String pkPlacement) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties props = KafkaConnectUtils.createCommonConnectorProperties();
    props.put("topic.prefix", connectorName);
    props.put("scylla.table.names", tableName);
    props.put("name", connectorName);
    props.put("cdc.output.format", "advanced");
    props.put("cdc.include.before", "full");
    props.put("cdc.include.after", "full");
    props.put("cdc.include.primary-key.placement", pkPlacement);
    KafkaConnectUtils.registerConnector(props, connectorName);
    consumer.subscribe(List.of(connectorName + "." + tableName));
    return consumer;
  }

  /**
   * Returns the name value for INSERT test. Default is "test_<pk>" which allows tests to identify
   * records by PK even when PK is not included in the message.
   */
  protected String insertNameValue(int pk) {
    return "test_" + pk;
  }

  /**
   * Returns the name value for DELETE test insert. Default is "delete_<pk>" which allows tests to
   * identify records by PK even when PK is not included in the message.
   */
  protected String deleteNameValue(int pk) {
    return "delete_" + pk;
  }

  /**
   * Returns the name value before UPDATE test. Default is "before_<pk>" which allows tests to
   * identify records by PK even when PK is not included in the message.
   */
  protected String updateBeforeNameValue(int pk) {
    return "before_" + pk;
  }

  /**
   * Returns the name value after UPDATE test. Default is "after_<pk>" which allows tests to
   * identify records by PK even when PK is not included in the message.
   */
  protected String updateAfterNameValue(int pk) {
    return "after_" + pk;
  }

  @Test
  void testInsert() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name) VALUES ("
            + pk
            + ", '"
            + insertNameValue(pk)
            + "')");
    waitAndAssert(pk, expectedInsert(pk));
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name) VALUES ("
            + pk
            + ", '"
            + deleteNameValue(pk)
            + "')");
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk);
    waitAndAssert(pk, expectedDelete(pk));
  }

  @Test
  void testUpdate() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name) VALUES ("
            + pk
            + ", '"
            + updateBeforeNameValue(pk)
            + "')");
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET name = '"
            + updateAfterNameValue(pk)
            + "' WHERE id = "
            + pk);
    waitAndAssert(pk, expectedUpdate(pk));
  }
}
