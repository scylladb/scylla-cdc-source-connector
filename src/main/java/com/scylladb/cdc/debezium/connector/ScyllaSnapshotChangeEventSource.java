package com.scylladb.cdc.debezium.connector;

import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;

public class ScyllaSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource {

    private final SnapshotProgressListener snapshotProgressListener;

    public ScyllaSnapshotChangeEventSource(ScyllaConnectorConfig connectorConfig, ScyllaOffsetContext previousOffset, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, snapshotProgressListener);
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    protected SnapshotResult doExecute(ChangeEventSourceContext changeEventSourceContext, SnapshotContext snapshotContext, SnapshottingTask snapshottingTask) throws Exception {
        snapshotProgressListener.snapshotCompleted();
        return SnapshotResult.completed(snapshotContext.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext offsetContext) {
        return new SnapshottingTask(false, false);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {

    }
}
