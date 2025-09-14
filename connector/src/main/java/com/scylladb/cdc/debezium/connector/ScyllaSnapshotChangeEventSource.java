package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import java.util.Collections;

public class ScyllaSnapshotChangeEventSource
    extends AbstractSnapshotChangeEventSource<ScyllaPartition, ScyllaOffsetContext> {

  private final SnapshotProgressListener<ScyllaPartition> snapshotProgressListener;

  public ScyllaSnapshotChangeEventSource(
      ScyllaConnectorConfig connectorConfig,
      SnapshotProgressListener<ScyllaPartition> snapshotProgressListener,
      NotificationService<ScyllaPartition, ScyllaOffsetContext> notificationService) {
    super(connectorConfig, snapshotProgressListener, notificationService);
    this.snapshotProgressListener = snapshotProgressListener;
  }

  @Override
  protected SnapshotResult<ScyllaOffsetContext> doExecute(
      ChangeEventSourceContext changeEventSourceContext,
      ScyllaOffsetContext previousOffset,
      SnapshotContext<ScyllaPartition, ScyllaOffsetContext> snapshotContext,
      SnapshottingTask snapshottingTask)
      throws Exception {
    snapshotProgressListener.snapshotCompleted(snapshotContext.partition);
    return SnapshotResult.completed(previousOffset);
  }

  @Override
  public SnapshottingTask getSnapshottingTask(
      ScyllaPartition partition, ScyllaOffsetContext offsetContext) {
    return new SnapshottingTask(
        false, false, Collections.emptyList(), Collections.emptyMap(), false);
  }

  @Override
  public SnapshottingTask getBlockingSnapshottingTask(
      ScyllaPartition partition,
      ScyllaOffsetContext offsetContext,
      SnapshotConfiguration snapshotConfiguration) {
    return new SnapshottingTask(
        false, false, Collections.emptyList(), Collections.emptyMap(), true);
  }

  @Override
  protected SnapshotContext<ScyllaPartition, ScyllaOffsetContext> prepare(
      ScyllaPartition partition, boolean onDemand) throws Exception {
    return new SnapshotContext<>(partition);
  }
}
