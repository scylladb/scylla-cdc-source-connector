/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Ported from Debezium 2.7.4 (io.debezium.transforms.extractnewstate.LegacyDeleteHandlingStrategy).
 */
package com.scylladb.cdc.debezium.connector.transforms;

import io.debezium.transforms.extractnewstate.AbstractExtractRecordStrategy;
import java.util.Arrays;
import java.util.Locale;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;

/**
 * The {@code drop.tombstones} / {@code delete.handling.mode} behaviour of Debezium 2.x's {@code
 * LegacyDeleteHandlingStrategy}, which Debezium 3.2 removed together with both options. Kept so
 * that existing connector configurations keep their output after the move to Debezium 3.x.
 *
 * @author Harvey Yue
 */
class LegacyDeleteHandlingStrategy<R extends ConnectRecord<R>>
    extends AbstractExtractRecordStrategy<R> {

  static final String DROP_TOMBSTONES = "drop.tombstones";
  static final String DELETE_HANDLING_MODE = "delete.handling.mode";
  static final boolean DEFAULT_DROP_TOMBSTONES = true;

  enum DeleteHandling {
    DROP,
    REWRITE,
    NONE;

    static final DeleteHandling DEFAULT = DROP;

    String value() {
      return name().toLowerCase(Locale.ROOT);
    }

    static String[] literals() {
      return Arrays.stream(values()).map(DeleteHandling::value).toArray(String[]::new);
    }

    static DeleteHandling parse(String value) {
      return value == null
          ? DEFAULT
          : DeleteHandling.valueOf(value.trim().toUpperCase(Locale.ROOT));
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
