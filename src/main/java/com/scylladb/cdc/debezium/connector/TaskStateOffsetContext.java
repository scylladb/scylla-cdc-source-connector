package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class TaskStateOffsetContext implements OffsetContext {
  private final ScyllaOffsetContext scyllaOffsetContext;
  final SourceInfo sourceInfo;

  public TaskStateOffsetContext(ScyllaOffsetContext scyllaOffsetContext, SourceInfo sourceInfo) {
    this.scyllaOffsetContext = scyllaOffsetContext;
    this.sourceInfo = sourceInfo;
  }

  @Override
  public Map<String, ?> getOffset() {
    return sourceInfo.offset();
  }

  public TaskState getTaskState() {
    return sourceInfo.getTaskState();
  }

  public void dataChangeEvent(TaskState taskState) {
    sourceInfo.dataChangeEvent(taskState);
  }

  @Override
  public Schema getSourceInfoSchema() {
    return sourceInfo.schema();
  }

  @Override
  public Struct getSourceInfo() {
    return sourceInfo.struct();
  }

  @Override
  public boolean isSnapshotRunning() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markSnapshotRecord(SnapshotRecord snapshotRecord) {}

  @Override
  public void preSnapshotStart() {}

  @Override
  public void preSnapshotCompletion() {}

  @Override
  public void postSnapshotCompletion() {}

  @Override
  public void event(DataCollectionId dataCollectionId, Instant instant) {
    // Not used by the Scylla CDC Source Connector.
    throw new UnsupportedOperationException();
  }

  @Override
  public TransactionContext getTransactionContext() {
    return scyllaOffsetContext.getTransactionContext();
  }
}
