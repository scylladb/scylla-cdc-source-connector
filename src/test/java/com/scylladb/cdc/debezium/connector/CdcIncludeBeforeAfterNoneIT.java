package com.scylladb.cdc.debezium.connector;

import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromAfter;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromJson;
import static com.scylladb.cdc.debezium.connector.JsonTestUtils.extractIdFromKeyField;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Integration tests for cdc.include.before=none and cdc.include.after=none.
 *
 * <p>Tests verify that when both before and after are set to "none", the messages do not contain
 * before/after data (both are null), relying only on operation type and primary key for consumers
 * to understand what happened.
 *
 * <p>Expected behavior by operation type:
 *
 * <ul>
 *   <li>INSERT: before=null, after=null
 *   <li>UPDATE: before=null, after=null
 *   <li>DELETE: before=null, after=null
 * </ul>
 *
 * <p>The "none" mode is useful for scenarios where only the fact that a change occurred matters,
 * not the actual data values. This can significantly reduce message size.
 */
public class CdcIncludeBeforeAfterNoneIT extends CdcIncludeBeforeAfterBase<String, String> {

  private static final String BEFORE_MODE = "none";
  private static final String AFTER_MODE = "none";

  @Override
  KafkaConsumer<String, String> buildConsumer(String connectorName, String tableName) {
    return buildStringConsumer(connectorName, tableName, BEFORE_MODE, AFTER_MODE);
  }

  @Override
  protected int extractPkFromValue(String value) {
    // With none mode, we can only rely on the kafka key for PK extraction
    // But try after first in case it's present due to other configs
    int pk = extractIdFromAfter(value);
    if (pk != -1) {
      return pk;
    }
    // For none mode, extract from the "key" field
    return extractIdFromKeyField(value);
  }

  @Override
  protected int extractPkFromKey(String key) {
    return extractIdFromJson(key);
  }

  /**
   * INSERT: before=null, after=null.
   *
   * <p>With mode=none, no data is included even for INSERT operations. The "key" field contains the
   * PK values for identification.
   */
  @Override
  String[] expectedInsert(int pk) {
    return new String[] {buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())};
  }

  /**
   * DELETE: before=null, after=null.
   *
   * <p>With mode=none, no data is included for DELETE operations. The "key" field contains the PK
   * values for identification.
   */
  @Override
  String[] expectedDelete(int pk) {
    return new String[] {
      // INSERT record (none mode)
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // DELETE record (none mode)
      buildDeleteRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // Tombstone record (null value) for Kafka log compaction
      null
    };
  }

  /**
   * UPDATE (partial): before=null, after=null.
   *
   * <p>With mode=none, no data is included for UPDATE operations. The "key" field contains the PK
   * values for identification.
   */
  @Override
  String[] expectedUpdate(int pk) {
    return new String[] {
      // INSERT record (none mode)
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record (none mode)
      buildUpdateRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }

  /**
   * UPDATE (all active columns): before=null, after=null.
   *
   * <p>With mode=none, no data is included regardless of how many columns were modified. The "key"
   * field contains the PK values for identification.
   */
  @Override
  String[] expectedUpdateMultiColumn(int pk) {
    return new String[] {
      // INSERT record (none mode)
      buildInsertRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource()),
      // UPDATE record (none mode)
      buildUpdateMultiColumnRecord(pk, BEFORE_MODE, AFTER_MODE, expectedSource())
    };
  }
}
