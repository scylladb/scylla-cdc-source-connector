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
 * should only include modified columns in before/after structs for UPDATE operations. Every type
 * has an "untouched_" counterpart that is set on INSERT but never modified, to verify that
 * only-updated mode correctly excludes unchanged columns.
 *
 * <p>Each test uses a unique primary key (obtained via {@link #reservePk()}) for isolation,
 * allowing all tests to share a single connector and table.
 *
 * @param <K> the type of the Kafka consumer key
 * @param <V> the type of the Kafka consumer value
 */
public abstract class CdcIncludeBeforeAfterBase<K, V> extends ScyllaTypesIT<K, V> {

  // ===================== Untouched Column Values =====================
  // These values are set on INSERT and never modified during UPDATE operations.

  static final String UNTOUCHED_ASCII = "untouched_ascii";
  static final long UNTOUCHED_BIGINT = 9999999999999L;
  static final String UNTOUCHED_BLOB_HEX = "0xDEADCAFE";
  static final boolean UNTOUCHED_BOOLEAN = true;
  static final String UNTOUCHED_DATE = "2020-01-01";
  static final String UNTOUCHED_DECIMAL = "99999.99";
  static final double UNTOUCHED_DOUBLE = 9.99999;
  static final String UNTOUCHED_DURATION = "9d9h9m";
  static final float UNTOUCHED_FLOAT = 9.99f;
  static final String UNTOUCHED_INET = "192.168.1.1";
  static final int UNTOUCHED_INT = 99999;
  static final short UNTOUCHED_SMALLINT = 999;
  static final String UNTOUCHED_TEXT = "untouched_text";
  static final String UNTOUCHED_TIME = "09:09:09.999";
  static final String UNTOUCHED_TIMESTAMP = "2020-01-01T09:09:09.999Z";
  static final String UNTOUCHED_TIMEUUID = "a1a1a1a1-a1a1-11f0-a1a1-a1a1a1a1a1a1";
  static final byte UNTOUCHED_TINYINT = 99;
  static final String UNTOUCHED_UUID = "99999999-9999-9999-9999-999999999999";
  static final String UNTOUCHED_VARCHAR = "untouched_varchar";
  static final String UNTOUCHED_VARINT = "9999999999";
  // Untouched collections
  static final String UNTOUCHED_LIST = "[999, 998, 997]";
  static final String UNTOUCHED_SET = "{'untouched_a', 'untouched_b'}";
  static final String UNTOUCHED_MAP = "{999: 'untouched_val'}";
  static final String UNTOUCHED_FROZEN_LIST = "[999, 998]";
  static final String UNTOUCHED_FROZEN_SET = "{'untouched_x', 'untouched_y'}";
  static final String UNTOUCHED_FROZEN_MAP = "{999: 'untouched_frozen'}";
  static final String UNTOUCHED_FROZEN_TUPLE = "(999, 'untouched_tuple')";
  // Untouched UDTs
  static final String UNTOUCHED_FROZEN_UDT = "{a: 999, b: 'untouched_udt'}";
  static final String UNTOUCHED_NF_UDT = "{a: 998, b: 'untouched_nf_udt'}";
  static final String UNTOUCHED_FROZEN_NESTED_UDT =
      "{inner: {x: 999, y: 'untouched_inner'}, z: 999}";
  static final String UNTOUCHED_NF_NESTED_UDT =
      "{inner: {x: 998, y: 'untouched_nf_inner'}, z: 998}";
  static final String UNTOUCHED_FROZEN_UDT_WITH_MAP =
      "{m: {81d4a034-4632-11f0-9484-409dd8f36eba: 'untouched_value'}}";
  static final String UNTOUCHED_NF_UDT_WITH_MAP =
      "{m: {81d4a035-4632-11f0-9484-409dd8f36eba: 'untouched_nf_value'}}";
  static final String UNTOUCHED_FROZEN_UDT_WITH_LIST = "{l: [999, 998]}";
  static final String UNTOUCHED_NF_UDT_WITH_LIST = "{l: [997, 996]}";
  static final String UNTOUCHED_FROZEN_UDT_WITH_SET = "{s: {'untouched_set_a', 'untouched_set_b'}}";
  static final String UNTOUCHED_NF_UDT_WITH_SET =
      "{s: {'untouched_nf_set_a', 'untouched_nf_set_b'}}";

  // ===================== Abstract Methods =====================

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

  // ===================== Schema Definition =====================

  /**
   * Creates UDT types before table creation (from ScyllaTypesUDTBase).
   *
   * @param keyspaceName the keyspace name
   */
  @Override
  protected void createTypesBeforeTable(String keyspaceName) {
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".simple_udt (a int, b text);");
    session.execute("CREATE TYPE IF NOT EXISTS " + keyspaceName + ".inner_udt (x int, y text);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".nested_udt (inner frozen<inner_udt>, z int);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS "
            + keyspaceName
            + ".udt_with_map (m frozen<map<timeuuid, text>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_list (l frozen<list<int>>);");
    session.execute(
        "CREATE TYPE IF NOT EXISTS " + keyspaceName + ".udt_with_set (s frozen<set<text>>);");
  }

  /**
   * Table schema with all supported data types. Each type has both an active column (modified in
   * tests) and an untouched counterpart (set once on INSERT, never modified).
   */
  @Override
  protected String createTableCql(String tableName) {
    return "("
        + "id int PRIMARY KEY,"
        // Primitive types - active columns
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
        + "varint_col varint,"
        // Primitive types - untouched columns
        + "untouched_ascii ascii,"
        + "untouched_bigint bigint,"
        + "untouched_blob blob,"
        + "untouched_boolean boolean,"
        + "untouched_date date,"
        + "untouched_decimal decimal,"
        + "untouched_double double,"
        + "untouched_duration duration,"
        + "untouched_float float,"
        + "untouched_inet inet,"
        + "untouched_int int,"
        + "untouched_smallint smallint,"
        + "untouched_text text,"
        + "untouched_time time,"
        + "untouched_timestamp timestamp,"
        + "untouched_timeuuid timeuuid,"
        + "untouched_tinyint tinyint,"
        + "untouched_uuid uuid,"
        + "untouched_varchar varchar,"
        + "untouched_varint varint,"
        // Non-frozen collections - active columns
        + "list_col list<int>,"
        + "set_col set<text>,"
        + "map_col map<int, text>,"
        // Non-frozen collections - untouched columns
        + "untouched_list list<int>,"
        + "untouched_set set<text>,"
        + "untouched_map map<int, text>,"
        // Frozen collections - active columns
        + "frozen_list_col frozen<list<int>>,"
        + "frozen_set_col frozen<set<text>>,"
        + "frozen_map_col frozen<map<int, text>>,"
        + "frozen_tuple_col frozen<tuple<int, text>>,"
        // Frozen collections - untouched columns
        + "untouched_frozen_list frozen<list<int>>,"
        + "untouched_frozen_set frozen<set<text>>,"
        + "untouched_frozen_map frozen<map<int, text>>,"
        + "untouched_frozen_tuple frozen<tuple<int, text>>,"
        // UDTs - active columns
        + "frozen_udt frozen<simple_udt>,"
        + "nf_udt simple_udt,"
        + "frozen_nested_udt frozen<nested_udt>,"
        + "nf_nested_udt nested_udt,"
        + "frozen_udt_with_map frozen<udt_with_map>,"
        + "nf_udt_with_map udt_with_map,"
        + "frozen_udt_with_list frozen<udt_with_list>,"
        + "nf_udt_with_list udt_with_list,"
        + "frozen_udt_with_set frozen<udt_with_set>,"
        + "nf_udt_with_set udt_with_set,"
        // UDTs - untouched columns
        + "untouched_frozen_udt frozen<simple_udt>,"
        + "untouched_nf_udt simple_udt,"
        + "untouched_frozen_nested_udt frozen<nested_udt>,"
        + "untouched_nf_nested_udt nested_udt,"
        + "untouched_frozen_udt_with_map frozen<udt_with_map>,"
        + "untouched_nf_udt_with_map udt_with_map,"
        + "untouched_frozen_udt_with_list frozen<udt_with_list>,"
        + "untouched_nf_udt_with_list udt_with_list,"
        + "untouched_frozen_udt_with_set frozen<udt_with_set>,"
        + "untouched_nf_udt_with_set udt_with_set"
        + ")";
  }

  // ===================== CQL Column Lists =====================

  /** All column names for INSERT operations. */
  private static final String ALL_COLUMNS =
      "id, "
          // Primitive types - active
          + "ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, "
          + "duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, "
          + "timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col, "
          // Primitive types - untouched
          + "untouched_ascii, untouched_bigint, untouched_blob, untouched_boolean, untouched_date, "
          + "untouched_decimal, untouched_double, untouched_duration, untouched_float, "
          + "untouched_inet, untouched_int, untouched_smallint, untouched_text, untouched_time, "
          + "untouched_timestamp, untouched_timeuuid, untouched_tinyint, untouched_uuid, "
          + "untouched_varchar, untouched_varint, "
          // Non-frozen collections - active
          + "list_col, set_col, map_col, "
          // Non-frozen collections - untouched
          + "untouched_list, untouched_set, untouched_map, "
          // Frozen collections - active
          + "frozen_list_col, frozen_set_col, frozen_map_col, frozen_tuple_col, "
          // Frozen collections - untouched
          + "untouched_frozen_list, untouched_frozen_set, untouched_frozen_map, "
          + "untouched_frozen_tuple, "
          // UDTs - active
          + "frozen_udt, nf_udt, frozen_nested_udt, nf_nested_udt, frozen_udt_with_map, "
          + "nf_udt_with_map, frozen_udt_with_list, nf_udt_with_list, frozen_udt_with_set, "
          + "nf_udt_with_set, "
          // UDTs - untouched
          + "untouched_frozen_udt, untouched_nf_udt, untouched_frozen_nested_udt, "
          + "untouched_nf_nested_udt, untouched_frozen_udt_with_map, untouched_nf_udt_with_map, "
          + "untouched_frozen_udt_with_list, untouched_nf_udt_with_list, "
          + "untouched_frozen_udt_with_set, untouched_nf_udt_with_set";

  // ===================== CQL Value Sets =====================

  /** Values for active primitive columns (set 1 - initial values). */
  private static final String PRIMITIVE_VALUES_SET1 =
      "'ascii_val', 1234567890123, 0xCAFEBABE, true, '2024-06-10', 12345.67, 3.14159, 1d12h30m, "
          + "2.71828, '127.0.0.1', 42, 7, 'text_val', '12:34:56.789', '2024-06-10T12:34:56.789Z', "
          + "81d4a030-4632-11f0-9484-409dd8f36eba, 5, 453662fa-db4b-4938-9033-d8523c0a371c, "
          + "'varchar_val', 999999999";

  /** Values for untouched primitive columns (never modified). */
  private static final String UNTOUCHED_PRIMITIVE_VALUES =
      "'"
          + UNTOUCHED_ASCII
          + "', "
          + UNTOUCHED_BIGINT
          + ", "
          + UNTOUCHED_BLOB_HEX
          + ", "
          + UNTOUCHED_BOOLEAN
          + ", '"
          + UNTOUCHED_DATE
          + "', "
          + UNTOUCHED_DECIMAL
          + ", "
          + UNTOUCHED_DOUBLE
          + ", "
          + UNTOUCHED_DURATION
          + ", "
          + UNTOUCHED_FLOAT
          + ", '"
          + UNTOUCHED_INET
          + "', "
          + UNTOUCHED_INT
          + ", "
          + UNTOUCHED_SMALLINT
          + ", '"
          + UNTOUCHED_TEXT
          + "', '"
          + UNTOUCHED_TIME
          + "', '"
          + UNTOUCHED_TIMESTAMP
          + "', "
          + UNTOUCHED_TIMEUUID
          + ", "
          + UNTOUCHED_TINYINT
          + ", "
          + UNTOUCHED_UUID
          + ", '"
          + UNTOUCHED_VARCHAR
          + "', "
          + UNTOUCHED_VARINT;

  /** Values for active non-frozen collections (set 1). */
  private static final String NF_COLLECTION_VALUES_SET1 =
      "[10, 20, 30], {'x', 'y', 'z'}, {1: 'one', 2: 'two'}";

  /** Values for untouched non-frozen collections. */
  private static final String UNTOUCHED_NF_COLLECTION_VALUES =
      UNTOUCHED_LIST + ", " + UNTOUCHED_SET + ", " + UNTOUCHED_MAP;

  /** Values for active frozen collections (set 1). */
  private static final String FROZEN_COLLECTION_VALUES_SET1 =
      "[100, 200], {'p', 'q'}, {10: 'ten'}, (1, 'tuple1')";

  /** Values for untouched frozen collections. */
  private static final String UNTOUCHED_FROZEN_COLLECTION_VALUES =
      UNTOUCHED_FROZEN_LIST
          + ", "
          + UNTOUCHED_FROZEN_SET
          + ", "
          + UNTOUCHED_FROZEN_MAP
          + ", "
          + UNTOUCHED_FROZEN_TUPLE;

  /** Values for active UDTs (set 1). */
  private static final String UDT_VALUES_SET1 =
      "{a: 42, b: 'foo'}, "
          + "{a: 7, b: 'bar'}, "
          + "{inner: {x: 10, y: 'hello'}, z: 20}, "
          + "{inner: {x: 11, y: 'world'}, z: 21}, "
          + "{m: {81d4a030-4632-11f0-9484-409dd8f36eba: 'value1', 81d4a031-4632-11f0-9484-409dd8f36eba: 'value2'}}, "
          + "{m: {81d4a032-4632-11f0-9484-409dd8f36eba: 'value3'}}, "
          + "{l: [1, 2, 3]}, "
          + "{l: [4, 5]}, "
          + "{s: {'a', 'b', 'c'}}, "
          + "{s: {'d', 'e'}}";

  /** Values for untouched UDTs. */
  private static final String UNTOUCHED_UDT_VALUES =
      UNTOUCHED_FROZEN_UDT
          + ", "
          + UNTOUCHED_NF_UDT
          + ", "
          + UNTOUCHED_FROZEN_NESTED_UDT
          + ", "
          + UNTOUCHED_NF_NESTED_UDT
          + ", "
          + UNTOUCHED_FROZEN_UDT_WITH_MAP
          + ", "
          + UNTOUCHED_NF_UDT_WITH_MAP
          + ", "
          + UNTOUCHED_FROZEN_UDT_WITH_LIST
          + ", "
          + UNTOUCHED_NF_UDT_WITH_LIST
          + ", "
          + UNTOUCHED_FROZEN_UDT_WITH_SET
          + ", "
          + UNTOUCHED_NF_UDT_WITH_SET;

  // ===================== CQL Update SET Clauses =====================

  /** SET clause for updating all active primitive columns to set 2 values. */
  private static final String SET_PRIMITIVES_TO_SET2 =
      "ascii_col = 'ascii_val2', bigint_col = 1234567890124, blob_col = 0xDEADBEEF, "
          + "boolean_col = false, date_col = '2024-06-11', decimal_col = 98765.43, "
          + "double_col = 2.71828, duration_col = 2d1h, float_col = 1.41421, "
          + "inet_col = '127.0.0.2', int_col = 43, smallint_col = 8, text_col = 'text_val2', "
          + "time_col = '01:02:03.456', timestamp_col = '2024-06-11T01:02:03.456Z', "
          + "timeuuid_col = 81d4a031-4632-11f0-9484-409dd8f36eba, tinyint_col = 6, "
          + "uuid_col = 453662fa-db4b-4938-9033-d8523c0a371d, varchar_col = 'varchar_val2', "
          + "varint_col = 888888888";

  /** SET clause for updating all active non-frozen collections to set 2 values. */
  private static final String SET_NF_COLLECTIONS_TO_SET2 =
      "list_col = [40, 50], set_col = {'a', 'b'}, map_col = {3: 'three'}";

  /** SET clause for updating all active frozen collections to set 2 values. */
  private static final String SET_FROZEN_COLLECTIONS_TO_SET2 =
      "frozen_list_col = [300, 400], frozen_set_col = {'r', 's'}, "
          + "frozen_map_col = {20: 'twenty'}, frozen_tuple_col = (2, 'tuple2')";

  /** SET clause for updating all active UDTs to set 2 values. */
  private static final String SET_UDTS_TO_SET2 =
      "frozen_udt = {a: 99, b: 'updated'}, "
          + "nf_udt = {a: 88, b: 'updated_nf'}, "
          + "frozen_nested_udt = {inner: {x: 50, y: 'updated_inner'}, z: 60}, "
          + "nf_nested_udt = {inner: {x: 51, y: 'updated_nf_inner'}, z: 61}, "
          + "frozen_udt_with_map = {m: {81d4a036-4632-11f0-9484-409dd8f36eba: 'newvalue'}}, "
          + "nf_udt_with_map = {m: {81d4a037-4632-11f0-9484-409dd8f36eba: 'newvalue2'}}, "
          + "frozen_udt_with_list = {l: [7, 8, 9]}, "
          + "nf_udt_with_list = {l: [10, 11]}, "
          + "frozen_udt_with_set = {s: {'x', 'y'}}, "
          + "nf_udt_with_set = {s: {'z'}}";

  /** SET clause for updating only primitive columns (for single-category update test). */
  private static final String SET_ONLY_PRIMITIVES =
      "ascii_col = 'ascii_val2', text_col = 'text_val2', int_col = 43";

  // ===================== Consumer Setup =====================

  /**
   * Builds a KafkaConsumer with the specified before/after mode configuration.
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
    props.put(
        "cdc.include.primary-key.placement", "kafka-key,payload-after,payload-before,payload-key");
    KafkaConnectUtils.registerConnector(props, connectorName);
    consumer.subscribe(List.of(connectorName + "." + tableName));
    return consumer;
  }

  // ===================== CQL Helper Methods =====================

  /** Builds complete VALUES clause for INSERT with all columns using set 1 values. */
  private String buildInsertValuesSet1(int pk) {
    return pk
        + ", "
        + PRIMITIVE_VALUES_SET1
        + ", "
        + UNTOUCHED_PRIMITIVE_VALUES
        + ", "
        + NF_COLLECTION_VALUES_SET1
        + ", "
        + UNTOUCHED_NF_COLLECTION_VALUES
        + ", "
        + FROZEN_COLLECTION_VALUES_SET1
        + ", "
        + UNTOUCHED_FROZEN_COLLECTION_VALUES
        + ", "
        + UDT_VALUES_SET1
        + ", "
        + UNTOUCHED_UDT_VALUES;
  }

  /** Inserts a full row with all columns using set 1 values. */
  protected void insertFullRow(int pk) {
    session.execute(
        "INSERT INTO "
            + getSuiteKeyspaceTableName()
            + " ("
            + ALL_COLUMNS
            + ") VALUES ("
            + buildInsertValuesSet1(pk)
            + ")");
  }

  /** Deletes a row by primary key. */
  protected void deleteRow(int pk) {
    session.execute("DELETE FROM " + getSuiteKeyspaceTableName() + " WHERE id = " + pk);
  }

  /** Updates all active columns (primitives, collections, UDTs) to set 2 values. */
  protected void updateAllActiveColumns(int pk) {
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + SET_PRIMITIVES_TO_SET2
            + ", "
            + SET_NF_COLLECTIONS_TO_SET2
            + ", "
            + SET_FROZEN_COLLECTIONS_TO_SET2
            + ", "
            + SET_UDTS_TO_SET2
            + " WHERE id = "
            + pk);
  }

  /** Updates only a subset of primitive columns (for testing only-updated mode). */
  protected void updateSomePrimitives(int pk) {
    session.execute(
        "UPDATE "
            + getSuiteKeyspaceTableName()
            + " SET "
            + SET_ONLY_PRIMITIVES
            + " WHERE id = "
            + pk);
  }

  // ===================== Test Methods =====================

  @Test
  void testInsert() {
    int pk = reservePk();
    insertFullRow(pk);
    waitAndAssert(pk, expectedInsert(pk));
  }

  @Test
  void testDelete() {
    int pk = reservePk();
    insertFullRow(pk);
    deleteRow(pk);
    waitAndAssert(pk, expectedDelete(pk));
  }

  /** Tests UPDATE operation that modifies only a few columns (for testing only-updated mode). */
  @Test
  void testUpdateSingleColumn() {
    int pk = reservePk();
    insertFullRow(pk);
    updateSomePrimitives(pk);
    waitAndAssert(pk, expectedUpdate(pk));
  }

  /** Tests UPDATE operation that modifies all active columns. */
  @Test
  void testUpdateMultiColumn() {
    int pk = reservePk();
    insertFullRow(pk);
    updateAllActiveColumns(pk);
    waitAndAssert(pk, expectedUpdateMultiColumn(pk));
  }

  // ===================== Expected JSON Value Constants =====================
  // These are the expected JSON representations of values in Kafka records.
  // Note: JSON representations may differ from CQL values (e.g., blob is base64 encoded).

  // --- Set 1 Values (Initial/Before UPDATE) ---
  static final String JSON_ASCII_SET1 = "ascii_val";
  static final long JSON_BIGINT_SET1 = 1234567890123L;
  static final String JSON_BLOB_SET1 = "yv66vg=="; // base64 of 0xCAFEBABE
  static final boolean JSON_BOOLEAN_SET1 = true;
  static final int JSON_DATE_SET1 = 19884; // days since epoch for 2024-06-10
  static final String JSON_DECIMAL_SET1 = "12345.67";
  static final double JSON_DOUBLE_SET1 = 3.14159;
  static final String JSON_DURATION_SET1 = "1d12h30m";
  static final double JSON_FLOAT_SET1 = 2.71828;
  static final String JSON_INET_SET1 = "127.0.0.1";
  static final int JSON_INT_SET1 = 42;
  static final int JSON_SMALLINT_SET1 = 7;
  static final String JSON_TEXT_SET1 = "text_val";
  static final long JSON_TIME_SET1 = 45296789000000L; // nanoseconds for 12:34:56.789
  static final long JSON_TIMESTAMP_SET1 = 1718022896789L; // millis for 2024-06-10T12:34:56.789Z
  static final String JSON_TIMEUUID_SET1 = "81d4a030-4632-11f0-9484-409dd8f36eba";
  static final int JSON_TINYINT_SET1 = 5;
  static final String JSON_UUID_SET1 = "453662fa-db4b-4938-9033-d8523c0a371c";
  static final String JSON_VARCHAR_SET1 = "varchar_val";
  static final String JSON_VARINT_SET1 = "999999999";

  // --- Set 2 Values (After UPDATE) ---
  static final String JSON_ASCII_SET2 = "ascii_val2";
  static final long JSON_BIGINT_SET2 = 1234567890124L;
  static final String JSON_BLOB_SET2 = "3q2+7w=="; // base64 of 0xDEADBEEF
  static final boolean JSON_BOOLEAN_SET2 = false;
  static final int JSON_DATE_SET2 = 19885; // days since epoch for 2024-06-11
  static final String JSON_DECIMAL_SET2 = "98765.43";
  static final double JSON_DOUBLE_SET2 = 2.71828;
  static final String JSON_DURATION_SET2 = "2d1h";
  static final double JSON_FLOAT_SET2 = 1.41421;
  static final String JSON_INET_SET2 = "127.0.0.2";
  static final int JSON_INT_SET2 = 43;
  static final int JSON_SMALLINT_SET2 = 8;
  static final String JSON_TEXT_SET2 = "text_val2";
  static final long JSON_TIME_SET2 = 3723456000000L; // nanoseconds for 01:02:03.456
  static final long JSON_TIMESTAMP_SET2 = 1718067723456L; // millis for 2024-06-11T01:02:03.456Z
  static final String JSON_TIMEUUID_SET2 = "81d4a031-4632-11f0-9484-409dd8f36eba";
  static final int JSON_TINYINT_SET2 = 6;
  static final String JSON_UUID_SET2 = "453662fa-db4b-4938-9033-d8523c0a371d";
  static final String JSON_VARCHAR_SET2 = "varchar_val2";
  static final String JSON_VARINT_SET2 = "888888888";

  // --- Untouched Values (JSON representations) ---
  static final String JSON_UNTOUCHED_ASCII = UNTOUCHED_ASCII;
  static final long JSON_UNTOUCHED_BIGINT = UNTOUCHED_BIGINT;
  static final String JSON_UNTOUCHED_BLOB = "3q3K/g=="; // base64 of 0xDEADCAFE
  static final boolean JSON_UNTOUCHED_BOOLEAN = UNTOUCHED_BOOLEAN;
  static final int JSON_UNTOUCHED_DATE = 18262; // days since epoch for 2020-01-01
  static final String JSON_UNTOUCHED_DECIMAL = UNTOUCHED_DECIMAL;
  static final double JSON_UNTOUCHED_DOUBLE = UNTOUCHED_DOUBLE;
  static final String JSON_UNTOUCHED_DURATION = UNTOUCHED_DURATION;
  static final double JSON_UNTOUCHED_FLOAT = 9.99; // JSON uses the raw float value
  static final String JSON_UNTOUCHED_INET = UNTOUCHED_INET;
  static final int JSON_UNTOUCHED_INT = UNTOUCHED_INT;
  static final int JSON_UNTOUCHED_SMALLINT = UNTOUCHED_SMALLINT;
  static final String JSON_UNTOUCHED_TEXT = UNTOUCHED_TEXT;
  static final long JSON_UNTOUCHED_TIME = 32949999000000L; // nanoseconds for 09:09:09.999
  static final long JSON_UNTOUCHED_TIMESTAMP =
      1577869749999L; // millis for 2020-01-01T09:09:09.999Z
  static final String JSON_UNTOUCHED_TIMEUUID = UNTOUCHED_TIMEUUID;
  static final int JSON_UNTOUCHED_TINYINT = UNTOUCHED_TINYINT;
  static final String JSON_UNTOUCHED_UUID = UNTOUCHED_UUID;
  static final String JSON_UNTOUCHED_VARCHAR = UNTOUCHED_VARCHAR;
  static final String JSON_UNTOUCHED_VARINT = UNTOUCHED_VARINT;

  // ===================== JSON Builder Helpers =====================

  /**
   * Builds the expected JSON for all primitive columns with Set 1 values.
   *
   * @param pk the primary key value
   * @return JSON fragment for all primitives (active + untouched) with Set 1 values
   */
  protected static String jsonPrimitivesSet1(int pk) {
    return """
        "id": %d,
        "ascii_col": "%s",
        "bigint_col": %d,
        "blob_col": "%s",
        "boolean_col": %s,
        "date_col": %d,
        "decimal_col": "%s",
        "double_col": %s,
        "duration_col": "%s",
        "float_col": %s,
        "inet_col": "%s",
        "int_col": %d,
        "smallint_col": %d,
        "text_col": "%s",
        "time_col": %d,
        "timestamp_col": %d,
        "timeuuid_col": "%s",
        "tinyint_col": %d,
        "uuid_col": "%s",
        "varchar_col": "%s",
        "varint_col": "%s\""""
        .formatted(
            pk,
            JSON_ASCII_SET1,
            JSON_BIGINT_SET1,
            JSON_BLOB_SET1,
            JSON_BOOLEAN_SET1,
            JSON_DATE_SET1,
            JSON_DECIMAL_SET1,
            JSON_DOUBLE_SET1,
            JSON_DURATION_SET1,
            JSON_FLOAT_SET1,
            JSON_INET_SET1,
            JSON_INT_SET1,
            JSON_SMALLINT_SET1,
            JSON_TEXT_SET1,
            JSON_TIME_SET1,
            JSON_TIMESTAMP_SET1,
            JSON_TIMEUUID_SET1,
            JSON_TINYINT_SET1,
            JSON_UUID_SET1,
            JSON_VARCHAR_SET1,
            JSON_VARINT_SET1);
  }

  /**
   * Builds the expected JSON for all primitive columns with Set 2 values.
   *
   * @param pk the primary key value
   * @return JSON fragment for all primitives (active only) with Set 2 values
   */
  protected static String jsonPrimitivesSet2(int pk) {
    return """
        "id": %d,
        "ascii_col": "%s",
        "bigint_col": %d,
        "blob_col": "%s",
        "boolean_col": %s,
        "date_col": %d,
        "decimal_col": "%s",
        "double_col": %s,
        "duration_col": "%s",
        "float_col": %s,
        "inet_col": "%s",
        "int_col": %d,
        "smallint_col": %d,
        "text_col": "%s",
        "time_col": %d,
        "timestamp_col": %d,
        "timeuuid_col": "%s",
        "tinyint_col": %d,
        "uuid_col": "%s",
        "varchar_col": "%s",
        "varint_col": "%s\""""
        .formatted(
            pk,
            JSON_ASCII_SET2,
            JSON_BIGINT_SET2,
            JSON_BLOB_SET2,
            JSON_BOOLEAN_SET2,
            JSON_DATE_SET2,
            JSON_DECIMAL_SET2,
            JSON_DOUBLE_SET2,
            JSON_DURATION_SET2,
            JSON_FLOAT_SET2,
            JSON_INET_SET2,
            JSON_INT_SET2,
            JSON_SMALLINT_SET2,
            JSON_TEXT_SET2,
            JSON_TIME_SET2,
            JSON_TIMESTAMP_SET2,
            JSON_TIMEUUID_SET2,
            JSON_TINYINT_SET2,
            JSON_UUID_SET2,
            JSON_VARCHAR_SET2,
            JSON_VARINT_SET2);
  }

  /** Builds JSON for untouched primitive columns. */
  protected static String jsonUntouchedPrimitives() {
    return """
        "untouched_ascii": "%s",
        "untouched_bigint": %d,
        "untouched_blob": "%s",
        "untouched_boolean": %s,
        "untouched_date": %d,
        "untouched_decimal": "%s",
        "untouched_double": %s,
        "untouched_duration": "%s",
        "untouched_float": %s,
        "untouched_inet": "%s",
        "untouched_int": %d,
        "untouched_smallint": %d,
        "untouched_text": "%s",
        "untouched_time": %d,
        "untouched_timestamp": %d,
        "untouched_timeuuid": "%s",
        "untouched_tinyint": %d,
        "untouched_uuid": "%s",
        "untouched_varchar": "%s",
        "untouched_varint": "%s\""""
        .formatted(
            JSON_UNTOUCHED_ASCII,
            JSON_UNTOUCHED_BIGINT,
            JSON_UNTOUCHED_BLOB,
            JSON_UNTOUCHED_BOOLEAN,
            JSON_UNTOUCHED_DATE,
            JSON_UNTOUCHED_DECIMAL,
            JSON_UNTOUCHED_DOUBLE,
            JSON_UNTOUCHED_DURATION,
            JSON_UNTOUCHED_FLOAT,
            JSON_UNTOUCHED_INET,
            JSON_UNTOUCHED_INT,
            JSON_UNTOUCHED_SMALLINT,
            JSON_UNTOUCHED_TEXT,
            JSON_UNTOUCHED_TIME,
            JSON_UNTOUCHED_TIMESTAMP,
            JSON_UNTOUCHED_TIMEUUID,
            JSON_UNTOUCHED_TINYINT,
            JSON_UNTOUCHED_UUID,
            JSON_UNTOUCHED_VARCHAR,
            JSON_UNTOUCHED_VARINT);
  }

  /** Builds JSON for non-frozen collections with Set 1 values. */
  protected static String jsonNfCollectionsSet1() {
    // Note: Maps are serialized as arrays of {"key": ..., "value": ...} objects
    return """
        "list_col": [10, 20, 30],
        "set_col": ["x", "y", "z"],
        "map_col": [{"key": 1, "value": "one"}, {"key": 2, "value": "two"}]""";
  }

  /** Builds JSON for non-frozen collections with Set 2 values. */
  protected static String jsonNfCollectionsSet2() {
    return """
        "list_col": [40, 50],
        "set_col": ["a", "b"],
        "map_col": [{"key": 3, "value": "three"}]""";
  }

  /** Builds JSON for untouched non-frozen collections. */
  protected static String jsonUntouchedNfCollections() {
    return """
        "untouched_list": [999, 998, 997],
        "untouched_set": ["untouched_a", "untouched_b"],
        "untouched_map": [{"key": 999, "value": "untouched_val"}]""";
  }

  /** Builds JSON for frozen collections with Set 1 values. */
  protected static String jsonFrozenCollectionsSet1() {
    // Note: Maps are serialized as arrays of {"key": ..., "value": ...} objects
    // Note: Tuples are serialized as objects with "field_N" keys for Avro compatibility
    return """
        "frozen_list_col": [100, 200],
        "frozen_set_col": ["p", "q"],
        "frozen_map_col": [{"key": 10, "value": "ten"}],
        "frozen_tuple_col": {"field_0": 1, "field_1": "tuple1"}""";
  }

  /** Builds JSON for frozen collections with Set 2 values. */
  protected static String jsonFrozenCollectionsSet2() {
    return """
        "frozen_list_col": [300, 400],
        "frozen_set_col": ["r", "s"],
        "frozen_map_col": [{"key": 20, "value": "twenty"}],
        "frozen_tuple_col": {"field_0": 2, "field_1": "tuple2"}""";
  }

  /** Builds JSON for untouched frozen collections. */
  protected static String jsonUntouchedFrozenCollections() {
    return """
        "untouched_frozen_list": [999, 998],
        "untouched_frozen_set": ["untouched_x", "untouched_y"],
        "untouched_frozen_map": [{"key": 999, "value": "untouched_frozen"}],
        "untouched_frozen_tuple": {"field_0": 999, "field_1": "untouched_tuple"}""";
  }

  /** Builds JSON for UDTs with Set 1 values. */
  protected static String jsonUdtsSet1() {
    // Note: Maps inside UDTs are also serialized as arrays of {"key": ..., "value": ...} objects
    return """
        "frozen_udt": {"a": 42, "b": "foo"},
        "nf_udt": {"a": 7, "b": "bar"},
        "frozen_nested_udt": {"inner": {"x": 10, "y": "hello"}, "z": 20},
        "nf_nested_udt": {"inner": {"x": 11, "y": "world"}, "z": 21},
        "frozen_udt_with_map": {"m": [{"key": "81d4a030-4632-11f0-9484-409dd8f36eba", "value": "value1"}, {"key": "81d4a031-4632-11f0-9484-409dd8f36eba", "value": "value2"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a032-4632-11f0-9484-409dd8f36eba", "value": "value3"}]},
        "frozen_udt_with_list": {"l": [1, 2, 3]},
        "nf_udt_with_list": {"l": [4, 5]},
        "frozen_udt_with_set": {"s": ["a", "b", "c"]},
        "nf_udt_with_set": {"s": ["d", "e"]}""";
  }

  /** Builds JSON for UDTs with Set 2 values. */
  protected static String jsonUdtsSet2() {
    return """
        "frozen_udt": {"a": 99, "b": "updated"},
        "nf_udt": {"a": 88, "b": "updated_nf"},
        "frozen_nested_udt": {"inner": {"x": 50, "y": "updated_inner"}, "z": 60},
        "nf_nested_udt": {"inner": {"x": 51, "y": "updated_nf_inner"}, "z": 61},
        "frozen_udt_with_map": {"m": [{"key": "81d4a036-4632-11f0-9484-409dd8f36eba", "value": "newvalue"}]},
        "nf_udt_with_map": {"m": [{"key": "81d4a037-4632-11f0-9484-409dd8f36eba", "value": "newvalue2"}]},
        "frozen_udt_with_list": {"l": [7, 8, 9]},
        "nf_udt_with_list": {"l": [10, 11]},
        "frozen_udt_with_set": {"s": ["x", "y"]},
        "nf_udt_with_set": {"s": ["z"]}""";
  }

  /** Builds JSON for untouched UDTs. */
  protected static String jsonUntouchedUdts() {
    return """
        "untouched_frozen_udt": {"a": 999, "b": "untouched_udt"},
        "untouched_nf_udt": {"a": 998, "b": "untouched_nf_udt"},
        "untouched_frozen_nested_udt": {"inner": {"x": 999, "y": "untouched_inner"}, "z": 999},
        "untouched_nf_nested_udt": {"inner": {"x": 998, "y": "untouched_nf_inner"}, "z": 998},
        "untouched_frozen_udt_with_map": {"m": [{"key": "81d4a034-4632-11f0-9484-409dd8f36eba", "value": "untouched_value"}]},
        "untouched_nf_udt_with_map": {"m": [{"key": "81d4a035-4632-11f0-9484-409dd8f36eba", "value": "untouched_nf_value"}]},
        "untouched_frozen_udt_with_list": {"l": [999, 998]},
        "untouched_nf_udt_with_list": {"l": [997, 996]},
        "untouched_frozen_udt_with_set": {"s": ["untouched_set_a", "untouched_set_b"]},
        "untouched_nf_udt_with_set": {"s": ["untouched_nf_set_a", "untouched_nf_set_b"]}""";
  }

  // ===================== Full Image Builders (for mode=full) =====================

  /**
   * Builds a complete JSON object with all columns using Set 1 values. Used for "full" mode after
   * INSERT or before UPDATE.
   *
   * @param pk the primary key value
   * @return complete JSON object string with all columns
   */
  protected static String jsonFullImageSet1(int pk) {
    return "{\n"
        + jsonPrimitivesSet1(pk)
        + ",\n"
        + jsonUntouchedPrimitives()
        + ",\n"
        + jsonNfCollectionsSet1()
        + ",\n"
        + jsonUntouchedNfCollections()
        + ",\n"
        + jsonFrozenCollectionsSet1()
        + ",\n"
        + jsonUntouchedFrozenCollections()
        + ",\n"
        + jsonUdtsSet1()
        + ",\n"
        + jsonUntouchedUdts()
        + "\n}";
  }

  /**
   * Builds a complete JSON object with active columns using Set 2 values and untouched columns
   * unchanged. Used for "full" mode after UPDATE.
   *
   * @param pk the primary key value
   * @return complete JSON object string with all columns (active=Set2, untouched=unchanged)
   */
  protected static String jsonFullImageSet2(int pk) {
    return "{\n"
        + jsonPrimitivesSet2(pk)
        + ",\n"
        + jsonUntouchedPrimitives()
        + ",\n"
        + jsonNfCollectionsSet2()
        + ",\n"
        + jsonUntouchedNfCollections()
        + ",\n"
        + jsonFrozenCollectionsSet2()
        + ",\n"
        + jsonUntouchedFrozenCollections()
        + ",\n"
        + jsonUdtsSet2()
        + ",\n"
        + jsonUntouchedUdts()
        + "\n}";
  }

  // ===================== Partial Update Full Image Builder =====================

  /**
   * Builds JSON for primitive columns after a partial update (only ascii_col, text_col, int_col
   * changed to Set 2, all other primitives remain Set 1).
   *
   * @param pk the primary key value
   * @return JSON fragment for primitives after partial update
   */
  protected static String jsonPrimitivesPartialUpdate(int pk) {
    return """
        "id": %d,
        "ascii_col": "%s",
        "bigint_col": %d,
        "blob_col": "%s",
        "boolean_col": %s,
        "date_col": %d,
        "decimal_col": "%s",
        "double_col": %s,
        "duration_col": "%s",
        "float_col": %s,
        "inet_col": "%s",
        "int_col": %d,
        "smallint_col": %d,
        "text_col": "%s",
        "time_col": %d,
        "timestamp_col": %d,
        "timeuuid_col": "%s",
        "tinyint_col": %d,
        "uuid_col": "%s",
        "varchar_col": "%s",
        "varint_col": "%s\""""
        .formatted(
            pk,
            JSON_ASCII_SET2, // changed
            JSON_BIGINT_SET1, // unchanged
            JSON_BLOB_SET1, // unchanged
            JSON_BOOLEAN_SET1, // unchanged
            JSON_DATE_SET1, // unchanged
            JSON_DECIMAL_SET1, // unchanged
            JSON_DOUBLE_SET1, // unchanged
            JSON_DURATION_SET1, // unchanged
            JSON_FLOAT_SET1, // unchanged
            JSON_INET_SET1, // unchanged
            JSON_INT_SET2, // changed
            JSON_SMALLINT_SET1, // unchanged
            JSON_TEXT_SET2, // changed
            JSON_TIME_SET1, // unchanged
            JSON_TIMESTAMP_SET1, // unchanged
            JSON_TIMEUUID_SET1, // unchanged
            JSON_TINYINT_SET1, // unchanged
            JSON_UUID_SET1, // unchanged
            JSON_VARCHAR_SET1, // unchanged
            JSON_VARINT_SET1); // unchanged
  }

  /**
   * Builds a complete JSON object for the full image after a partial update. Only ascii_col,
   * text_col, and int_col are changed to Set 2 values; all other columns retain their original
   * values.
   *
   * @param pk the primary key value
   * @return complete JSON object string with partial update applied
   */
  protected static String jsonFullImagePartialUpdate(int pk) {
    return "{\n"
        + jsonPrimitivesPartialUpdate(pk)
        + ",\n"
        + jsonUntouchedPrimitives()
        + ",\n"
        + jsonNfCollectionsSet1() // collections unchanged
        + ",\n"
        + jsonUntouchedNfCollections()
        + ",\n"
        + jsonFrozenCollectionsSet1() // frozen collections unchanged
        + ",\n"
        + jsonUntouchedFrozenCollections()
        + ",\n"
        + jsonUdtsSet1() // UDTs unchanged
        + ",\n"
        + jsonUntouchedUdts()
        + "\n}";
  }

  // ===================== Only-Updated Image Builders (for mode=only-updated) =====================
  // Note: "only-updated" mode includes ALL columns in the JSON schema, but non-updated columns
  // are set to null. Only updated columns have actual values.

  /**
   * Builds JSON with all columns, where only the columns modified in updateSomePrimitives() have
   * values (Set 1), and all other columns are null. Used for "only-updated" mode before partial
   * UPDATE.
   *
   * @param pk the primary key value
   * @return JSON object with all columns (updated=Set1 values, non-updated=null)
   */
  protected static String jsonOnlyUpdatedPrimitivesBefore(int pk) {
    return """
        {
          "ascii_col": "%s",
          "bigint_col": null,
          "blob_col": null,
          "boolean_col": null,
          "date_col": null,
          "decimal_col": null,
          "double_col": null,
          "duration_col": null,
          "float_col": null,
          "frozen_list_col": null,
          "frozen_map_col": null,
          "frozen_nested_udt": null,
          "frozen_set_col": null,
          "frozen_tuple_col": null,
          "frozen_udt": null,
          "frozen_udt_with_list": null,
          "frozen_udt_with_map": null,
          "frozen_udt_with_set": null,
          "id": %d,
          "inet_col": null,
          "int_col": %d,
          "list_col": null,
          "map_col": null,
          "nf_nested_udt": null,
          "nf_udt": null,
          "nf_udt_with_list": null,
          "nf_udt_with_map": null,
          "nf_udt_with_set": null,
          "set_col": null,
          "smallint_col": null,
          "text_col": "%s",
          "time_col": null,
          "timestamp_col": null,
          "timeuuid_col": null,
          "tinyint_col": null,
          "untouched_ascii": null,
          "untouched_bigint": null,
          "untouched_blob": null,
          "untouched_boolean": null,
          "untouched_date": null,
          "untouched_decimal": null,
          "untouched_double": null,
          "untouched_duration": null,
          "untouched_float": null,
          "untouched_frozen_list": null,
          "untouched_frozen_map": null,
          "untouched_frozen_nested_udt": null,
          "untouched_frozen_set": null,
          "untouched_frozen_tuple": null,
          "untouched_frozen_udt": null,
          "untouched_frozen_udt_with_list": null,
          "untouched_frozen_udt_with_map": null,
          "untouched_frozen_udt_with_set": null,
          "untouched_inet": null,
          "untouched_int": null,
          "untouched_list": null,
          "untouched_map": null,
          "untouched_nf_nested_udt": null,
          "untouched_nf_udt": null,
          "untouched_nf_udt_with_list": null,
          "untouched_nf_udt_with_map": null,
          "untouched_nf_udt_with_set": null,
          "untouched_set": null,
          "untouched_smallint": null,
          "untouched_text": null,
          "untouched_time": null,
          "untouched_timestamp": null,
          "untouched_timeuuid": null,
          "untouched_tinyint": null,
          "untouched_uuid": null,
          "untouched_varchar": null,
          "untouched_varint": null,
          "uuid_col": null,
          "varchar_col": null,
          "varint_col": null
        }"""
        .formatted(JSON_ASCII_SET1, pk, JSON_INT_SET1, JSON_TEXT_SET1);
  }

  /**
   * Builds JSON with all columns, where only the columns modified in updateSomePrimitives() have
   * values (Set 2), and all other columns are null. Used for "only-updated" mode after partial
   * UPDATE.
   *
   * @param pk the primary key value
   * @return JSON object with all columns (updated=Set2 values, non-updated=null)
   */
  protected static String jsonOnlyUpdatedPrimitivesAfter(int pk) {
    return """
        {
          "ascii_col": "%s",
          "bigint_col": null,
          "blob_col": null,
          "boolean_col": null,
          "date_col": null,
          "decimal_col": null,
          "double_col": null,
          "duration_col": null,
          "float_col": null,
          "frozen_list_col": null,
          "frozen_map_col": null,
          "frozen_nested_udt": null,
          "frozen_set_col": null,
          "frozen_tuple_col": null,
          "frozen_udt": null,
          "frozen_udt_with_list": null,
          "frozen_udt_with_map": null,
          "frozen_udt_with_set": null,
          "id": %d,
          "inet_col": null,
          "int_col": %d,
          "list_col": null,
          "map_col": null,
          "nf_nested_udt": null,
          "nf_udt": null,
          "nf_udt_with_list": null,
          "nf_udt_with_map": null,
          "nf_udt_with_set": null,
          "set_col": null,
          "smallint_col": null,
          "text_col": "%s",
          "time_col": null,
          "timestamp_col": null,
          "timeuuid_col": null,
          "tinyint_col": null,
          "untouched_ascii": null,
          "untouched_bigint": null,
          "untouched_blob": null,
          "untouched_boolean": null,
          "untouched_date": null,
          "untouched_decimal": null,
          "untouched_double": null,
          "untouched_duration": null,
          "untouched_float": null,
          "untouched_frozen_list": null,
          "untouched_frozen_map": null,
          "untouched_frozen_nested_udt": null,
          "untouched_frozen_set": null,
          "untouched_frozen_tuple": null,
          "untouched_frozen_udt": null,
          "untouched_frozen_udt_with_list": null,
          "untouched_frozen_udt_with_map": null,
          "untouched_frozen_udt_with_set": null,
          "untouched_inet": null,
          "untouched_int": null,
          "untouched_list": null,
          "untouched_map": null,
          "untouched_nf_nested_udt": null,
          "untouched_nf_udt": null,
          "untouched_nf_udt_with_list": null,
          "untouched_nf_udt_with_map": null,
          "untouched_nf_udt_with_set": null,
          "untouched_set": null,
          "untouched_smallint": null,
          "untouched_text": null,
          "untouched_time": null,
          "untouched_timestamp": null,
          "untouched_timeuuid": null,
          "untouched_tinyint": null,
          "untouched_uuid": null,
          "untouched_varchar": null,
          "untouched_varint": null,
          "uuid_col": null,
          "varchar_col": null,
          "varint_col": null
        }"""
        .formatted(JSON_ASCII_SET2, pk, JSON_INT_SET2, JSON_TEXT_SET2);
  }

  /** Builds JSON for null untouched primitive columns (used in only-updated mode). */
  protected static String jsonNullUntouchedPrimitives() {
    return """
        "untouched_ascii": null,
        "untouched_bigint": null,
        "untouched_blob": null,
        "untouched_boolean": null,
        "untouched_date": null,
        "untouched_decimal": null,
        "untouched_double": null,
        "untouched_duration": null,
        "untouched_float": null,
        "untouched_inet": null,
        "untouched_int": null,
        "untouched_smallint": null,
        "untouched_text": null,
        "untouched_time": null,
        "untouched_timestamp": null,
        "untouched_timeuuid": null,
        "untouched_tinyint": null,
        "untouched_uuid": null,
        "untouched_varchar": null,
        "untouched_varint": null""";
  }

  /** Builds JSON for null untouched non-frozen collections (used in only-updated mode). */
  protected static String jsonNullUntouchedNfCollections() {
    return """
        "untouched_list": null,
        "untouched_set": null,
        "untouched_map": null""";
  }

  /** Builds JSON for null untouched frozen collections (used in only-updated mode). */
  protected static String jsonNullUntouchedFrozenCollections() {
    return """
        "untouched_frozen_list": null,
        "untouched_frozen_set": null,
        "untouched_frozen_map": null,
        "untouched_frozen_tuple": null""";
  }

  /** Builds JSON for null untouched UDTs (used in only-updated mode). */
  protected static String jsonNullUntouchedUdts() {
    return """
        "untouched_frozen_udt": null,
        "untouched_nf_udt": null,
        "untouched_frozen_nested_udt": null,
        "untouched_nf_nested_udt": null,
        "untouched_frozen_udt_with_map": null,
        "untouched_nf_udt_with_map": null,
        "untouched_frozen_udt_with_list": null,
        "untouched_nf_udt_with_list": null,
        "untouched_frozen_udt_with_set": null,
        "untouched_nf_udt_with_set": null""";
  }

  /**
   * Builds JSON with all active columns (modified in updateAllActiveColumns) with Set 1 values, and
   * all untouched columns set to null. Used for "only-updated" mode before full UPDATE.
   *
   * @param pk the primary key value
   * @return JSON object with all columns (active=Set1 values, untouched=null)
   */
  protected static String jsonOnlyUpdatedAllActiveBefore(int pk) {
    return "{\n"
        + jsonPrimitivesSet1(pk)
        + ",\n"
        + jsonNullUntouchedPrimitives()
        + ",\n"
        + jsonNfCollectionsSet1()
        + ",\n"
        + jsonNullUntouchedNfCollections()
        + ",\n"
        + jsonFrozenCollectionsSet1()
        + ",\n"
        + jsonNullUntouchedFrozenCollections()
        + ",\n"
        + jsonUdtsSet1()
        + ",\n"
        + jsonNullUntouchedUdts()
        + "\n}";
  }

  /**
   * Builds JSON with all active columns (modified in updateAllActiveColumns) with Set 2 values, and
   * all untouched columns set to null. Used for "only-updated" mode after full UPDATE.
   *
   * @param pk the primary key value
   * @return JSON object with all columns (active=Set2 values, untouched=null)
   */
  protected static String jsonOnlyUpdatedAllActiveAfter(int pk) {
    return "{\n"
        + jsonPrimitivesSet2(pk)
        + ",\n"
        + jsonNullUntouchedPrimitives()
        + ",\n"
        + jsonNfCollectionsSet2()
        + ",\n"
        + jsonNullUntouchedNfCollections()
        + ",\n"
        + jsonFrozenCollectionsSet2()
        + ",\n"
        + jsonNullUntouchedFrozenCollections()
        + ",\n"
        + jsonUdtsSet2()
        + ",\n"
        + jsonNullUntouchedUdts()
        + "\n}";
  }

  // ===================== Key JSON Builder =====================

  /**
   * Builds the expected JSON for the key field.
   *
   * @param pk the primary key value
   * @return JSON object for the key
   */
  protected static String jsonKey(int pk) {
    return "{\"id\": %d}".formatted(pk);
  }

  // ===================== Complete Record Builders =====================

  /**
   * Builds a complete expected Kafka record JSON for INSERT operation.
   *
   * @param pk the primary key value
   * @param beforeMode the cdc.include.before mode (none, full, only-updated)
   * @param afterMode the cdc.include.after mode (none, full, only-updated)
   * @param source the expected source JSON (use expectedSource())
   * @return complete JSON record string
   */
  protected static String buildInsertRecord(
      int pk, String beforeMode, String afterMode, String source) {
    String before = "null"; // INSERT always has null before
    String after;
    switch (afterMode) {
      case "none":
        after = "null";
        break;
      case "only-updated":
      case "full":
        // For INSERT, all columns are "updated" so both modes include full image
        after = jsonFullImageSet1(pk);
        break;
      default:
        throw new IllegalArgumentException("Unknown afterMode: " + afterMode);
    }
    return buildRecord(pk, before, after, "c", source);
  }

  /**
   * Builds a complete expected Kafka record JSON for DELETE operation.
   *
   * @param pk the primary key value
   * @param beforeMode the cdc.include.before mode (none, full, only-updated)
   * @param afterMode the cdc.include.after mode (none, full, only-updated)
   * @param source the expected source JSON (use expectedSource())
   * @return complete JSON record string
   */
  protected static String buildDeleteRecord(
      int pk, String beforeMode, String afterMode, String source) {
    // Note: For partition-key-only tables, Scylla sends PARTITION_DELETE which has no preimage
    String before = "null"; // Partition delete has no preimage
    String after = "null"; // DELETE always has null after
    return buildRecord(pk, before, after, "d", source);
  }

  /**
   * Builds a complete expected Kafka record JSON for UPDATE operation (partial update).
   *
   * @param pk the primary key value
   * @param beforeMode the cdc.include.before mode (none, full, only-updated)
   * @param afterMode the cdc.include.after mode (none, full, only-updated)
   * @param source the expected source JSON (use expectedSource())
   * @return complete JSON record string
   */
  protected static String buildUpdateRecord(
      int pk, String beforeMode, String afterMode, String source) {
    String before = buildBeforeForUpdate(pk, beforeMode, false);
    String after = buildAfterForUpdate(pk, afterMode, false);
    return buildRecord(pk, before, after, "u", source);
  }

  /**
   * Builds a complete expected Kafka record JSON for UPDATE operation (full update).
   *
   * @param pk the primary key value
   * @param beforeMode the cdc.include.before mode (none, full, only-updated)
   * @param afterMode the cdc.include.after mode (none, full, only-updated)
   * @param source the expected source JSON (use expectedSource())
   * @return complete JSON record string
   */
  protected static String buildUpdateMultiColumnRecord(
      int pk, String beforeMode, String afterMode, String source) {
    String before = buildBeforeForUpdate(pk, beforeMode, true);
    String after = buildAfterForUpdate(pk, afterMode, true);
    return buildRecord(pk, before, after, "u", source);
  }

  /** Helper to build before value based on mode. */
  private static String buildBeforeForUpdate(int pk, String beforeMode, boolean fullUpdate) {
    switch (beforeMode) {
      case "none":
        return "null";
      case "only-updated":
        return fullUpdate
            ? jsonOnlyUpdatedAllActiveBefore(pk)
            : jsonOnlyUpdatedPrimitivesBefore(pk);
      case "full":
        return jsonFullImageSet1(pk);
      default:
        throw new IllegalArgumentException("Unknown beforeMode: " + beforeMode);
    }
  }

  /** Helper to build after value based on mode. */
  private static String buildAfterForUpdate(int pk, String afterMode, boolean fullUpdate) {
    switch (afterMode) {
      case "none":
        return "null";
      case "only-updated":
        return fullUpdate ? jsonOnlyUpdatedAllActiveAfter(pk) : jsonOnlyUpdatedPrimitivesAfter(pk);
      case "full":
        // For full mode, partial update only changes some columns
        // Full update changes all active columns
        return fullUpdate ? jsonFullImageSet2(pk) : jsonFullImagePartialUpdate(pk);
      default:
        throw new IllegalArgumentException("Unknown afterMode: " + afterMode);
    }
  }

  /** Helper to build a complete record JSON. */
  private static String buildRecord(int pk, String before, String after, String op, String source) {
    return """
        {
          "before": %s,
          "after": %s,
          "key": %s,
          "op": "%s",
          "source": %s
        }"""
        .formatted(before, after, jsonKey(pk), op, source);
  }
}
