package com.scylladb.cdc.debezium.connector.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.debezium.data.Envelope;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

/**
 * Delete and tombstone output of the SMT. Debezium 3.x removed {@code drop.tombstones} and {@code
 * delete.handling.mode}; these tests pin the 2.x output for both.
 */
class ScyllaExtractNewRecordStateTest {

  private static final Schema KEY_SCHEMA =
      SchemaBuilder.struct().name("ks.t.Key").field("id", Schema.INT32_SCHEMA).build();
  private static final Schema ROW_SCHEMA =
      SchemaBuilder.struct()
          .name("ks.t.Value")
          .optional()
          .field("id", Schema.INT32_SCHEMA)
          .field("v", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private static final Schema SOURCE_SCHEMA =
      SchemaBuilder.struct().name("source").field("table", Schema.STRING_SCHEMA).build();
  private static final Envelope ENVELOPE =
      Envelope.defineSchema()
          .withName("ks.t.Envelope")
          .withRecord(ROW_SCHEMA)
          .withSource(SOURCE_SCHEMA)
          .build();

  private static Struct key() {
    return new Struct(KEY_SCHEMA).put("id", 1);
  }

  private static Struct row() {
    return new Struct(ROW_SCHEMA).put("id", 1).put("v", "a");
  }

  private static Struct source() {
    return new Struct(SOURCE_SCHEMA).put("table", "t");
  }

  private static SourceRecord record(Struct value) {
    return new SourceRecord(
        Map.of(), Map.of(), "ks.t", 0, KEY_SCHEMA, key(), ENVELOPE.schema(), value);
  }

  private static SourceRecord insert() {
    return record(ENVELOPE.create(row(), source(), Instant.EPOCH));
  }

  /** A Scylla delete without preimage: before and after are both null. */
  private static SourceRecord deleteWithoutPreimage() {
    return record(ENVELOPE.delete(null, source(), Instant.EPOCH));
  }

  private static SourceRecord deleteWithPreimage() {
    return record(ENVELOPE.delete(row(), source(), Instant.EPOCH));
  }

  private static SourceRecord tombstone() {
    return new SourceRecord(Map.of(), Map.of(), "ks.t", 0, KEY_SCHEMA, key(), null, null);
  }

  private static ScyllaExtractNewRecordState<SourceRecord> smt(String... keyValues) {
    Map<String, String> configs = new HashMap<>();
    configs.put("cdc.output.format", "advanced");
    for (int i = 0; i < keyValues.length; i += 2) {
      configs.put(keyValues[i], keyValues[i + 1]);
    }
    ScyllaExtractNewRecordState<SourceRecord> smt = new ScyllaExtractNewRecordState<>();
    smt.configure(configs);
    return smt;
  }

  @Test
  void insert_isFlattened() {
    SourceRecord out = smt().apply(insert());
    assertEquals("a", ((Struct) out.value()).getString("v"));
  }

  @Test
  void defaults_dropDeleteAndTombstone() {
    ScyllaExtractNewRecordState<SourceRecord> smt = smt();
    assertNull(smt.apply(deleteWithoutPreimage()));
    assertNull(smt.apply(deleteWithPreimage()));
    assertNull(smt.apply(tombstone()));
  }

  @Test
  void dropTombstonesFalse_keepsTheTombstoneOfADeleteWithoutPreimage() {
    ScyllaExtractNewRecordState<SourceRecord> smt = smt("drop.tombstones", "false");
    assertNull(smt.apply(deleteWithoutPreimage()));
    SourceRecord tombstone = tombstone();
    assertSame(tombstone, smt.apply(tombstone));
  }

  @Test
  void deleteHandlingModeNone_emitsANullValueForADeleteWithPreimage() {
    SourceRecord out = smt("delete.handling.mode", "none").apply(deleteWithPreimage());
    assertNotNull(out);
    assertNull(out.value());
  }

  @Test
  void deleteHandlingModeRewrite_marksTheDeletedRow() {
    SourceRecord out = smt("delete.handling.mode", "rewrite").apply(deleteWithPreimage());
    assertEquals("true", ((Struct) out.value()).getString("__deleted"));
  }

  @Test
  void newOption_usesDebezium3Handling() {
    ScyllaExtractNewRecordState<SourceRecord> smt =
        smt("delete.tombstone.handling.mode", "tombstone", "drop.tombstones", "false");
    // tombstone mode drops the original tombstone and nulls the delete's value instead
    assertNull(smt.apply(tombstone()));
    SourceRecord out = smt.apply(deleteWithPreimage());
    assertNotNull(out);
    assertNull(out.value());
  }

  @Test
  void blankNewOption_fallsBackToTheLegacyOptions() {
    ScyllaExtractNewRecordState<SourceRecord> smt =
        smt("delete.tombstone.handling.mode", "", "drop.tombstones", "false");
    SourceRecord tombstone = tombstone();
    assertSame(tombstone, smt.apply(tombstone));
  }
}
