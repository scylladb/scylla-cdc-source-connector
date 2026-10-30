package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The deltas of {@code INSERT ... USING TTL} on a table with a non-frozen list {@code l} (#365):
 * the INSERT with {@code t}, then an UPDATE with the {@code cdc$deleted_l} tombstone, then an
 * UPDATE with the list cells.
 */
class MergedRawChangeTest {

  private final ChangeSchema schema = mock(ChangeSchema.class);
  private final ChangeSchema.ColumnDefinition t = column("t");
  private final ChangeSchema.ColumnDefinition l = column("l");
  private final ChangeSchema.ColumnDefinition deletedL = column("cdc$deleted_l");
  private final ChangeSchema.ColumnDefinition ttl = column("cdc$ttl");
  private final ChangeSchema.ColumnDefinition untouched = column("u");

  private final RawChange insert =
      delta(RawChange.OperationType.ROW_INSERT, 0, 600L, false, List.of(t, ttl));
  private final RawChange tombstone =
      delta(RawChange.OperationType.ROW_UPDATE, 1, null, false, List.of(deletedL));
  private final RawChange cells =
      delta(RawChange.OperationType.ROW_UPDATE, 2, 700L, true, List.of(l, ttl));

  private static ChangeSchema.ColumnDefinition column(String name) {
    ChangeSchema.ColumnDefinition cdef = mock(ChangeSchema.ColumnDefinition.class);
    when(cdef.getColumnName()).thenReturn(name);
    when(cdef.isCdcColumn()).thenReturn(name.startsWith("cdc$"));
    when(cdef.getBaseTableColumnKind()).thenReturn(ChangeSchema.ColumnKind.REGULAR);
    return cdef;
  }

  /** A delta setting {@code setColumns}; every other column is null. */
  private RawChange delta(
      RawChange.OperationType operationType,
      int batchSeqNo,
      Long ttlValue,
      boolean endOfBatch,
      List<ChangeSchema.ColumnDefinition> setColumns) {
    RawChange change = mock(RawChange.class);
    when(change.getOperationType()).thenReturn(operationType);
    when(change.getTTL()).thenReturn(ttlValue);
    when(change.isEndOfBatch()).thenReturn(endOfBatch);
    when(change.getBatchSequenceNumber()).thenReturn(batchSeqNo);
    when(change.getId()).thenReturn(mock(ChangeId.class));
    when(change.getSchema()).thenReturn(schema);
    when(change.isNull(any(ChangeSchema.ColumnDefinition.class)))
        .thenAnswer(invocation -> !setColumns.contains(invocation.getArgument(0)));
    when(change.getAsObject(any(ChangeSchema.ColumnDefinition.class)))
        .thenAnswer(invocation -> value(change, invocation.getArgument(0)));
    when(change.getAsUnsafeBytes(any(ChangeSchema.ColumnDefinition.class)))
        .thenAnswer(
            invocation -> ByteBuffer.wrap(value(change, invocation.getArgument(0)).getBytes()));
    when(change.getCell(any(ChangeSchema.ColumnDefinition.class)))
        .thenAnswer(invocation -> mock(Cell.class));
    return change;
  }

  /** Each delta reports a value naming itself, so the test can see which delta was read. */
  private String value(RawChange change, ChangeSchema.ColumnDefinition cdef) {
    String who = change == insert ? "insert" : change == tombstone ? "tombstone" : "cells";
    return who + ":" + cdef.getColumnName();
  }

  private RawChange merged() {
    return MergedRawChange.merge(MergedRawChange.merge(insert, tombstone), cells);
  }

  @Test
  void cdcMetadataComesFromTheFirstDelta() {
    RawChange merged = merged();
    assertEquals(RawChange.OperationType.ROW_INSERT, merged.getOperationType());
    assertEquals(600L, merged.getTTL());
    assertSame(insert.getId(), merged.getId());
    assertEquals(insert.getBatchSequenceNumber(), merged.getBatchSequenceNumber());
    assertFalse(merged.isEndOfBatch());
    assertSame(schema, merged.getSchema());
    assertEquals("insert:cdc$ttl", merged.getAsObject(ttl));
  }

  @Test
  void otherColumnsComeFromTheLastDeltaSettingThem() {
    RawChange merged = merged();
    assertEquals("insert:t", merged.getAsObject(t));
    assertEquals("tombstone:cdc$deleted_l", merged.getAsObject(deletedL));
    assertEquals("cells:l", merged.getAsObject(l));
    assertEquals(ByteBuffer.wrap("cells:l".getBytes()), merged.getAsUnsafeBytes(l));
    assertFalse(merged.isNull(l));
    assertFalse(merged.isNull(deletedL));
  }

  @Test
  void aColumnNoDeltaSetsIsNull() {
    RawChange merged = merged();
    assertTrue(merged.isNull(untouched));
    assertEquals("insert:u", merged.getAsObject(untouched));
  }

  /** Two additions to one collection with different TTLs: each delta holds only its own cells. */
  @Test
  void overlaps_whenADeltaSetsAColumnAlreadySet() {
    when(schema.getAllColumnDefinitions()).thenReturn(List.of(t, l, deletedL, ttl));
    assertFalse(MergedRawChange.overlaps(MergedRawChange.merge(insert, tombstone), cells));
    RawChange moreCells = delta(RawChange.OperationType.ROW_UPDATE, 3, 900L, true, List.of(l, ttl));
    assertTrue(MergedRawChange.overlaps(merged(), moreCells));
  }

  /** A non-frozen UDT's tombstone also sets the column, with every field null (#365). */
  @Test
  void overlaps_notWhenTheEarlierValueIsATombstone() {
    when(schema.getAllColumnDefinitions()).thenReturn(List.of(t, l, deletedL, ttl));
    RawChange udtTombstone =
        delta(RawChange.OperationType.ROW_UPDATE, 1, null, false, List.of(l, deletedL));
    assertFalse(MergedRawChange.overlaps(MergedRawChange.merge(insert, udtTombstone), cells));
  }

  @Test
  void mergeWithNull_replaces() {
    assertSame(insert, MergedRawChange.merge(null, insert));
    assertNull(MergedRawChange.merge(insert, null));
  }
}
