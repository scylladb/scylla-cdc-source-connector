package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.extractnewstate.AbstractExtractRecordStrategy;
import java.util.Locale;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;

/**
 * The {@code drop.tombstones} / {@code delete.handling.mode} behaviour of Debezium 2.x's {@code
 * LegacyDeleteHandlingStrategy}, which Debezium 3.0 removed together with both options.
 *
 * <p>Ported from Debezium 2.7 (Apache License 2.0) so that existing connector configurations keep
 * their output after the move to Debezium 3.x.
 */
class LegacyDeleteHandlingStrategy<R extends ConnectRecord<R>>
    extends AbstractExtractRecordStrategy<R> {

  static final String DROP_TOMBSTONES = "drop.tombstones";
  static final String DELETE_HANDLING_MODE = "delete.handling.mode";

  enum DeleteHandling {
    DROP,
    REWRITE,
    NONE;

    static DeleteHandling parse(String value) {
      return value == null ? DROP : DeleteHandling.valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
  }

  private final DeleteHandling deleteHandling;
  private final boolean dropTombstones;

  LegacyDeleteHandlingStrategy(
      DeleteHandling deleteHandling, boolean dropTombstones, boolean replaceNullWithDefault) {
    super(replaceNullWithDefault);
    this.deleteHandling = deleteHandling;
    this.dropTombstones = dropTombstones;
  }

  @Override
  public R handleTombstoneRecord(R record) {
    return dropTombstones ? null : record;
  }

  @Override
  public R handleDeleteRecord(R record) {
    switch (deleteHandling) {
      case DROP:
        return null;
      case NONE:
        return afterDelegate.apply(record);
      case REWRITE:
        R oldRecord = beforeDelegate.apply(record);
        return oldRecord.value() instanceof Struct ? removedDelegate.apply(oldRecord) : oldRecord;
      default:
        throw new IllegalStateException("Unknown delete handling mode: " + deleteHandling);
    }
  }

  @Override
  public R handleRecord(R record) {
    R newRecord = afterDelegate.apply(record);
    if (deleteHandling == DeleteHandling.REWRITE && newRecord.value() instanceof Struct) {
      return updatedDelegate.apply(newRecord);
    }
    return newRecord;
  }

  @Override
  public boolean isRewriteMode() {
    return deleteHandling == DeleteHandling.REWRITE;
  }
}
