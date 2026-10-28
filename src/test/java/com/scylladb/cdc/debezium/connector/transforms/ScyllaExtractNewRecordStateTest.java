package com.scylladb.cdc.debezium.connector.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.data.Envelope;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;
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

  private static SourceRecord update() {
    return record(ENVELOPE.update(row(), row(), source(), Instant.EPOCH));
  }

  // Legacy format: non-key columns are wrapped in Cell structs, and a delete always carries the
  // primary key in before, even without a preimage.
  private static final Schema CELL_SCHEMA =
      SchemaBuilder.struct()
          .name("ks.t.v.Cell")
          .optional()
          .field("value", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
  private static final Schema LEGACY_ROW_SCHEMA =
      SchemaBuilder.struct()
          .name("ks.t.Before")
          .optional()
          .field("id", Schema.INT32_SCHEMA)
          .field("v", CELL_SCHEMA)
          .build();
  private static final Envelope LEGACY_ENVELOPE =
      Envelope.defineSchema()
          .withName("ks.t.Envelope")
          .withRecord(LEGACY_ROW_SCHEMA)
          .withSource(SOURCE_SCHEMA)
          .build();

  private static SourceRecord legacyRecord(Struct value) {
    return new SourceRecord(
        Map.of(), Map.of(), "ks.t", 0, KEY_SCHEMA, key(), LEGACY_ENVELOPE.schema(), value);
  }

  private static SourceRecord legacyInsert() {
    Struct row =
        new Struct(LEGACY_ROW_SCHEMA)
            .put("id", 1)
            .put("v", new Struct(CELL_SCHEMA).put("value", "a"));
    return legacyRecord(LEGACY_ENVELOPE.create(row, source(), Instant.EPOCH));
  }

  private static SourceRecord legacyDelete() {
    Struct before = new Struct(LEGACY_ROW_SCHEMA).put("id", 1);
    return legacyRecord(LEGACY_ENVELOPE.delete(before, source(), Instant.EPOCH));
  }

  private static ScyllaExtractNewRecordState<SourceRecord> legacySmt(String... keyValues) {
    Map<String, String> configs = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      configs.put(keyValues[i], keyValues[i + 1]);
    }
    ScyllaExtractNewRecordState<SourceRecord> smt = new ScyllaExtractNewRecordState<>();
    smt.configure(configs);
    return smt;
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
  void deleteHandlingModeNone_emitsANullValueAndKeepsTheTombstone() {
    // Debezium 3.x's default would also null the delete, but it drops the tombstone
    ScyllaExtractNewRecordState<SourceRecord> smt =
        smt("delete.handling.mode", "none", "drop.tombstones", "false");
    SourceRecord out = smt.apply(deleteWithPreimage());
    assertNotNull(out);
    assertNull(out.value());
    SourceRecord tombstone = tombstone();
    assertSame(tombstone, smt.apply(tombstone));
  }

  @Test
  void deleteHandlingModeRewrite_marksInsertsAndUpdatesAsNotDeleted() {
    ScyllaExtractNewRecordState<SourceRecord> smt = smt("delete.handling.mode", "rewrite");
    assertEquals("false", ((Struct) smt.apply(insert()).value()).getString("__deleted"));
    assertEquals("false", ((Struct) smt.apply(update()).value()).getString("__deleted"));
  }

  @Test
  void deleteHandlingModeRewrite_withDropTombstonesFalse_keepsTheTombstone() {
    ScyllaExtractNewRecordState<SourceRecord> smt =
        smt("delete.handling.mode", "rewrite", "drop.tombstones", "false");
    assertEquals("true", ((Struct) smt.apply(deleteWithPreimage()).value()).getString("__deleted"));
    SourceRecord tombstone = tombstone();
    assertSame(tombstone, smt.apply(tombstone));
  }

  @Test
  void deleteHandlingModeRewrite_marksTheDeletedRow() {
    SourceRecord out = smt("delete.handling.mode", "rewrite").apply(deleteWithPreimage());
    assertEquals("true", ((Struct) out.value()).getString("__deleted"));
  }

  @Test
  void deleteHandlingMode_isCaseInsensitive() {
    // Debezium 2.x accepted any case
    ConfigValue value =
        new ScyllaExtractNewRecordState<SourceRecord>()
            .config().validate(Map.of("delete.handling.mode", "REWRITE")).stream()
                .filter(v -> v.name().equals("delete.handling.mode"))
                .findFirst()
                .orElseThrow();
    assertTrue(value.errorMessages().isEmpty(), value.errorMessages().toString());

    SourceRecord out = smt("delete.handling.mode", "REWRITE").apply(deleteWithPreimage());
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

  @Test
  void legacyInsert_unwrapsCells() {
    Struct value = (Struct) legacySmt().apply(legacyInsert()).value();
    assertEquals("a", value.getString("v"));
  }

  @Test
  void legacyDelete_defaults_dropDeleteAndTombstone() {
    ScyllaExtractNewRecordState<SourceRecord> smt = legacySmt();
    assertNull(smt.apply(legacyDelete()));
    assertNull(smt.apply(tombstone()));
  }

  @Test
  void legacyDelete_dropTombstonesFalse_keepsOnlyTheTombstone() {
    ScyllaExtractNewRecordState<SourceRecord> smt = legacySmt("drop.tombstones", "false");
    assertNull(smt.apply(legacyDelete()));
    SourceRecord tombstone = tombstone();
    assertSame(tombstone, smt.apply(tombstone));
  }

  @Test
  void legacyDelete_rewrite_marksTheKeyAsDeleted() {
    Struct value =
        (Struct) legacySmt("delete.handling.mode", "rewrite").apply(legacyDelete()).value();
    assertEquals(1, value.getInt32("id"));
    assertEquals("true", value.getString("__deleted"));
  }

  @Test
  void legacyDelete_newOptionTombstone_isANullValueNotADrop() {
    // Unlike an advanced-format delete without preimage, before is set, so it is no truncate
    SourceRecord out =
        legacySmt("delete.tombstone.handling.mode", "tombstone").apply(legacyDelete());
    assertNotNull(out);
    assertNull(out.value());
  }
}
