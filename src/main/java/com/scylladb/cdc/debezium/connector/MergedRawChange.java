package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The delta rows one CDC batch logs for one row, read as a single delta.
 *
 * <p>ScyllaDB can split one write into several deltas: {@code INSERT ... USING TTL} with a
 * non-frozen collection logs the INSERT, then the collection tombstone and the collection cells as
 * separate UPDATE rows (#365). The first delta carries the operation, so CDC metadata comes from
 * it; every other column comes from the last delta that sets it.
 */
final class MergedRawChange implements RawChange {
  private static final String CDC_PREFIX = "cdc$";
  private static final String CDC_DELETED_PREFIX = "cdc$deleted_";

  private final List<RawChange> deltas;

  private MergedRawChange(List<RawChange> deltas) {
    this.deltas = deltas;
  }

  /** Appends {@code next} to {@code current}; a null on either side replaces instead. */
  static RawChange merge(RawChange current, RawChange next) {
    if (current == null || next == null) {
      return next;
    }
    List<RawChange> deltas = new ArrayList<>();
    if (current instanceof MergedRawChange) {
      deltas.addAll(((MergedRawChange) current).deltas);
    } else {
      deltas.add(current);
    }
    deltas.add(next);
    return new MergedRawChange(deltas);
  }

  /**
   * Whether {@code next} sets a column {@code current} already sets. Merging them would keep only
   * {@code next}'s value, and a non-frozen collection delta holds only the cells it adds: ScyllaDB
   * logs two additions to one collection with different TTLs as two deltas of one batch. A column
   * set by its own tombstone does not count: the cells after a tombstone replace it (#365).
   */
  static boolean overlaps(RawChange current, RawChange next) {
    List<ChangeSchema.ColumnDefinition> columns = next.getSchema().getAllColumnDefinitions();
    for (ChangeSchema.ColumnDefinition cdef : columns) {
      boolean compared =
          cdef.isCdcColumn()
              ? cdef.getColumnName().startsWith(CDC_DELETED_PREFIX)
              : cdef.getBaseTableColumnKind() != ChangeSchema.ColumnKind.PARTITION_KEY
                  && cdef.getBaseTableColumnKind() != ChangeSchema.ColumnKind.CLUSTERING_KEY;
      if (!compared || current.isNull(cdef) || next.isNull(cdef)) {
        continue;
      }
      if (!cdef.isCdcColumn() && isTombstone(setter(current, cdef), cdef, columns)) {
        continue;
      }
      return true;
    }
    return false;
  }

  /** The delta of {@code change} that {@code cdef}'s value comes from. */
  private static RawChange setter(RawChange change, ChangeSchema.ColumnDefinition cdef) {
    return change instanceof MergedRawChange ? ((MergedRawChange) change).source(cdef) : change;
  }

  /**
   * Whether {@code delta} deletes {@code cdef}: a non-frozen UDT's tombstone also sets the column,
   * to a value with every field null.
   */
  private static boolean isTombstone(
      RawChange delta,
      ChangeSchema.ColumnDefinition cdef,
      List<ChangeSchema.ColumnDefinition> columns) {
    String deletedName = CDC_DELETED_PREFIX + cdef.getColumnName();
    for (ChangeSchema.ColumnDefinition column : columns) {
      if (column.getColumnName().equals(deletedName)) {
        return !delta.isNull(column);
      }
    }
    return false;
  }

  private RawChange first() {
    return deltas.get(0);
  }

  private RawChange source(ChangeSchema.ColumnDefinition cdef) {
    String name = cdef.getColumnName();
    if (name.startsWith(CDC_PREFIX) && !name.startsWith(CDC_DELETED_PREFIX)) {
      return first();
    }
    for (int i = deltas.size() - 1; i > 0; i--) {
      if (!deltas.get(i).isNull(cdef)) {
        return deltas.get(i);
      }
    }
    return first();
  }

  @Override
  public ChangeId getId() {
    return first().getId();
  }

  @Override
  public OperationType getOperationType() {
    return first().getOperationType();
  }

  @Override
  public boolean isEndOfBatch() {
    return first().isEndOfBatch();
  }

  @Override
  public int getBatchSequenceNumber() {
    return first().getBatchSequenceNumber();
  }

  @Override
  public Long getTTL() {
    return first().getTTL();
  }

  @Override
  public ChangeSchema getSchema() {
    return first().getSchema();
  }

  @Override
  public Object getAsObject(ChangeSchema.ColumnDefinition cdef) {
    return source(cdef).getAsObject(cdef);
  }

  @Override
  public Cell getCell(ChangeSchema.ColumnDefinition cdef) {
    return source(cdef).getCell(cdef);
  }

  @Override
  public boolean isNull(ChangeSchema.ColumnDefinition cdef) {
    return source(cdef).isNull(cdef);
  }

  @Override
  public ByteBuffer getAsUnsafeBytes(ChangeSchema.ColumnDefinition cdef) {
    return source(cdef).getAsUnsafeBytes(cdef);
  }
}
