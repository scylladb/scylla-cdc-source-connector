package com.scylladb.cdc.debezium.connector;

import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

/**
 * Base integration tests for cdc.include.before and cdc.include.after configuration options.
 *
 * <p>Tests verify that before and after values are correctly included or excluded in the Kafka
 * record based on the cdc.include.before and cdc.include.after configuration modes (none, full,
 * only-updated).
 *
 * <p>The table schema includes multiple columns to properly test the "only-updated" mode, which
 * should only include modified columns in before/after structs for UPDATE operations.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class CdcIncludeBeforeAfterBase<K, V> extends ScyllaTypesIT<K, V> {

  /**
   * Returns the expected JSON for an INSERT operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.before and
   * cdc.include.after configuration.
   */
  abstract String[] expectedInsert(int pk);

  /**
   * Returns the expected JSON for a DELETE operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.before and
   * cdc.include.after configuration.
   */
  abstract String[] expectedDelete(int pk);

  /**
   * Returns the expected JSON for an UPDATE operation with the given primary key. Implementations
   * should return the expected record structure based on their cdc.include.before and
   * cdc.include.after configuration.
   */
  abstract String[] expectedUpdate(int pk);

  /**
   * Returns the expected JSON for an UPDATE operation that modifies multiple columns.
   * Implementations should return the expected record structure based on their configuration.
   */
  abstract String[] expectedUpdateMultiColumn(int pk);

  /**
   * Table schema with multiple columns to test before/after modes properly. The schema includes: -
   * id: primary key - name: text column (used for test identification and single-column updates) -
   * value: integer column (used to test multi-column updates)
   */
  @Override
  protected String createTableCql(String tableName) {
    return "(id int PRIMARY KEY, name text, value int)";
  }

  /**
   * Builds a KafkaConsumer with the specified before/after mode configuration.
   *
   * <p>This helper method encapsulates the common connector setup pattern: creating properties,
   * setting CDC configuration, registering the connector, and subscribing to the topic.
   *
   * @param connectorName the name of the connector
   * @param tableName the table name to subscribe to
   * @param beforeMode the cdc.include.before configuration value (none, full, only-updated)
   * @param afterMode the cdc.include.after configuration value (none, full, only-updated)
   * @return a configured KafkaConsumer
   */
  protected static KafkaConsumer<String, String> buildStringConsumer(
      String connectorName, String tableName, String beforeMode, String afterMode) {
    KafkaConsumer<String, String> consumer = KafkaUtils.createStringConsumer();
    Properties props = KafkaConnectUtils.createCommonConnectorProperties();
    props.put("topic.prefix", connectorName);
    props.put("scylla.table.names", tableName);
    props.put("name", connectorName);
    props.put("cdc.output.format", "advanced");
    props.put("cdc.include.before", beforeMode);
    props.put("cdc.include.after", afterMode);
    // Include PK in all locations for easier test verification.
    // payload-key is needed for DELETE on partition-key-only tables where before=null.
    props.put(
        "cdc.include.primary-key.placement", "kafka-key,payload-after,payload-before,payload-key");
    KafkaConnectUtils.registerConnector(props, connectorName);
    consumer.subscribe(List.of(connectorName + "." + tableName));
    return consumer;
  }

  /** Returns the name value for INSERT test. Default is "test_<pk>". */
  protected String insertNameValue(int pk) {
    return "test_" + pk;
  }

  /** Returns the initial value for INSERT test. */
  protected int insertValueValue(int pk) {
    return 100 + pk;
  }

  /** Returns the name value for DELETE test insert. Default is "delete_<pk>". */
  protected String deleteNameValue(int pk) {
    return "delete_" + pk;
  }

  /** Returns the initial value for DELETE test. */
  protected int deleteValueValue(int pk) {
    return 200 + pk;
  }

  /** Returns the name value before UPDATE test. Default is "before_<pk>". */
  protected String updateBeforeNameValue(int pk) {
    return "before_" + pk;
  }

  /** Returns the name value after UPDATE test. Default is "after_<pk>". */
  protected String updateAfterNameValue(int pk) {
    return "after_" + pk;
  }

  /** Returns the initial value for UPDATE test (before update). */
  protected int updateBeforeValueValue(int pk) {
    return 300 + pk;
  }

  /** Returns the updated value for multi-column UPDATE test. */
  protected int updateAfterValueValue(int pk) {
    return 400 + pk;
  }

  @Test
  void testInsert() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name, value) VALUES ("
            + pk
            + ", '"
            + insertNameValue(pk)
            + "', "
            + insertValueValue(pk)
            + ")");
    waitAndAssert(pk, expectedInsert(pk));
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name, value) VALUES ("
            + pk
            + ", '"
            + deleteNameValue(pk)
            + "', "
            + deleteValueValue(pk)
            + ")");
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk);
    waitAndAssert(pk, expectedDelete(pk));
  }

  /** Tests UPDATE operation that modifies only a single column (name). */
  @Test
  void testUpdateSingleColumn() {
    int pk = reservePk();
    // Insert initial row with both name and value
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name, value) VALUES ("
            + pk
            + ", '"
            + updateBeforeNameValue(pk)
            + "', "
            + updateBeforeValueValue(pk)
            + ")");
    // Update only the name column (value stays unchanged)
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET name = '"
            + updateAfterNameValue(pk)
            + "' WHERE id = "
            + pk);
    waitAndAssert(pk, expectedUpdate(pk));
  }

  /** Tests UPDATE operation that modifies multiple columns (name and value). */
  @Test
  void testUpdateMultiColumn() {
    int pk = reservePk();
    // Insert initial row with both name and value
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " (id, name, value) VALUES ("
            + pk
            + ", '"
            + updateBeforeNameValue(pk)
            + "', "
            + updateBeforeValueValue(pk)
            + ")");
    // Update both name and value columns
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET name = '"
            + updateAfterNameValue(pk)
            + "', value = "
            + updateAfterValueValue(pk)
            + " WHERE id = "
            + pk);
    waitAndAssert(pk, expectedUpdateMultiColumn(pk));
  }
}
