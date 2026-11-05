package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The Debezium 3.x snapshot callbacks: the connector takes no snapshot, so they change nothing. */
class ScyllaOffsetContextSnapshotTest {

  private static final TaskId TASK_ID =
      new TaskId(
          new GenerationId(new Timestamp(new Date(1000))),
          new VNodeId(0),
          new TableName("ks", "t"));

  private static ScyllaOffsetContext offsetContext() {
    ScyllaConnectorConfig config =
        new ScyllaConnectorConfig(
            Configuration.create()
                .with("name", "test-connector")
                .with("topic.prefix", "test")
                .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
                .with("scylla.table.names", "ks.t")
                .build());
    SourceInfo sourceInfo = new SourceInfo(config, TASK_ID);
    sourceInfo.setTaskState(
        new TaskState(
            new Timestamp(new Date(10_000)), new Timestamp(new Date(20_000)), Optional.empty()));
    return new ScyllaOffsetContext(Map.of(TASK_ID, sourceInfo), new TransactionContext());
  }

  @Test
  void scyllaOffsetContext_reportsNoSnapshotAndIgnoresTheCallbacks() {
    ScyllaOffsetContext context = offsetContext();
    Map<String, ?> before = context.taskStateOffsetContext(TASK_ID).getOffset();

    runSnapshotCallbacks(context);

    assertEquals(before, context.taskStateOffsetContext(TASK_ID).getOffset());
  }

  @Test
  void taskStateOffsetContext_reportsNoSnapshotAndIgnoresTheCallbacks() {
    TaskStateOffsetContext context = offsetContext().taskStateOffsetContext(TASK_ID);
    Map<String, ?> before = context.getOffset();

    runSnapshotCallbacks(context);

    assertEquals(before, context.getOffset());
  }

  private static void runSnapshotCallbacks(OffsetContext context) {
    assertFalse(context.isInitialSnapshotRunning());
    context.preSnapshotStart(false);
    context.preSnapshotStart(true);
    assertFalse(context.isInitialSnapshotRunning());
    context.preSnapshotCompletion();
    context.postSnapshotCompletion();
    assertFalse(context.isInitialSnapshotRunning());
  }
}
