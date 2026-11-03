package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.cql.Cell;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  private final RawChange first;
  // For each column a later delta sets, the last delta setting it.
  private final Map<String, RawChange> setters = new HashMap<>();

  private MergedRawChange(RawChange first) {
    this.first = first;
  }

  /**
   * Appends {@code next} to {@code current}; a null on either side replaces instead. A merged
   * {@code current} is extended in place: its record keeps only the result.
   */
  static RawChange merge(RawChange current, RawChange next) {
    if (current == null || next == null) {
      return next;
    }
    MergedRawChange merged =
        current instanceof MergedRawChange
            ? (MergedRawChange) current
            : new MergedRawChange(current);
    for (ChangeSchema.ColumnDefinition cdef : next.getSchema().getAllColumnDefinitions()) {
      if (!next.isNull(cdef)) {
        merged.setters.put(cdef.getColumnName(), next);
      }
    }
    return merged;
  }

  /**
   * Whether {@code next} sets a column {@code current} already sets. Merging them would keep only
   * {@code next}'s value, and a non-frozen collection delta holds only the cells it adds: ScyllaDB
   * logs two additions to one collection with different TTLs as two deltas of one batch. A column
   * set by its own tombstone does not count: the cells after a tombstone replace it (#365).
   */
  static boolean overlaps(RawChange current, RawChange next) {
    List<ChangeSchema.ColumnDefinition> columns = next.getSchema().getAllColumnDefinitions();
    Map<String, ChangeSchema.ColumnDefinition> deletedColumns = null;
    for (ChangeSchema.ColumnDefinition cdef : columns) {
      boolean compared =
          cdef.isCdcColumn()
              ? cdef.getColumnName().startsWith(CDC_DELETED_PREFIX)
              : cdef.getBaseTableColumnKind() != ChangeSchema.ColumnKind.PARTITION_KEY
                  && cdef.getBaseTableColumnKind() != ChangeSchema.ColumnKind.CLUSTERING_KEY;
      if (!compared || next.isNull(cdef) || current.isNull(cdef)) {
        continue;
      }
      if (!cdef.isCdcColumn()) {
        if (deletedColumns == null) {
          deletedColumns = deletedColumns(columns);
        }
        ChangeSchema.ColumnDefinition deleted = deletedColumns.get(cdef.getColumnName());
        if (deleted != null && !setter(current, cdef).isNull(deleted)) {
          continue;
        }
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
   * Each column's {@code cdc$deleted_} column, by the column's name. A delta that sets it deletes
   * the column: a non-frozen UDT's tombstone also sets the column, to a value with every field
   * null.
   */
  private static Map<String, ChangeSchema.ColumnDefinition> deletedColumns(
      List<ChangeSchema.ColumnDefinition> columns) {
    Map<String, ChangeSchema.ColumnDefinition> deletedColumns = new HashMap<>();
    for (ChangeSchema.ColumnDefinition column : columns) {
      String name = column.getColumnName();
      if (name.startsWith(CDC_DELETED_PREFIX)) {
        deletedColumns.putIfAbsent(name.substring(CDC_DELETED_PREFIX.length()), column);
      }
    }
    return deletedColumns;
  }

  private RawChange source(ChangeSchema.ColumnDefinition cdef) {
    String name = cdef.getColumnName();
    if (name.startsWith(CDC_PREFIX) && !name.startsWith(CDC_DELETED_PREFIX)) {
      return first;
    }
    return setters.getOrDefault(name, first);
  }

  @Override
  public ChangeId getId() {
    return first.getId();
  }

  @Override
  public OperationType getOperationType() {
    return first.getOperationType();
  }

  @Override
  public boolean isEndOfBatch() {
    return first.isEndOfBatch();
  }

  @Override
  public int getBatchSequenceNumber() {
    return first.getBatchSequenceNumber();
  }

  @Override
  public Long getTTL() {
    return first.getTTL();
  }

  @Override
  public ChangeSchema getSchema() {
    return first.getSchema();
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
