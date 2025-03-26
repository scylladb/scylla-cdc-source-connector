package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

import java.util.Map;

public class ScyllaPartition implements Partition {
  private final ScyllaOffsetContext scyllaOffsetContext;
  private final SourceInfo sourceInfo;

  public ScyllaPartition(ScyllaOffsetContext scyllaOffsetContext, SourceInfo sourceInfo) {
    this.scyllaOffsetContext = scyllaOffsetContext;
    this.sourceInfo = sourceInfo;
  }

  @Override
  public Map<String, String> getSourcePartition() {
    return sourceInfo.partition();
  }
}
