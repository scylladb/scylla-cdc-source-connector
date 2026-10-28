package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.GenerationId;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.model.VNodeId;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.ChangeTime;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.RawChange.OperationType;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import com.scylladb.cdc.model.worker.TaskState;
import com.scylladb.cdc.model.worker.cql.Cell;
import io.debezium.config.Configuration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The offset carried by a dispatched record must never let a restart skip a CDC log row that has
 * not been dispatched yet (#340). Rows of one batch share a {@link ChangeId}, and the library
 * resumes strictly after the saved one, so a mid-batch offset loses the rest of the batch.
 */
class ScyllaChangesConsumerBatchOffsetTest {

  private static final TableName TABLE = new TableName("ks", "t");
  private static final TaskId TASK_ID =
      new TaskId(new GenerationId(new Timestamp(new Date(1000))), new VNodeId(0), TABLE);
  private static final StreamId STREAM = new StreamId(ByteBuffer.wrap(new byte[16]));
  private static final long WINDOW_START_MS = 10_000;
  private static final long WINDOW_END_MS = 20_000;

  /** What Debezium captures from the offset context while building a record. */
  private static final class Snapshot {
    final int rowIndex;
    final Map<String, String> offset;
    final long tsUs;
    final Instant tsMs;

    Snapshot(int rowIndex, Map<String, String> offset, long tsUs, Instant tsMs) {
      this.rowIndex = rowIndex;
      this.offset = offset;
      this.tsUs = tsUs;
      this.tsMs = tsMs;
    }
  }

  private final List<Snapshot> snapshots = new ArrayList<>();
  private int currentRow;
  private SourceInfo sourceInfo;
  private ScyllaOffsetContext offsetContext;
  private EventDispatcher<ScyllaPartition, CollectionId> dispatcher;

  private ScyllaConnectorConfig config(CdcIncludeMode before, CdcIncludeMode after) {
    return new ScyllaConnectorConfig(
        Configuration.create()
            .with("name", "test-connector")
            .with("topic.prefix", "test")
            .with("scylla.cluster.ip.addresses", "127.0.0.1:9042")
            .with("scylla.table.names", "ks.t")
            .with(ScyllaConnectorConfig.CDC_INCLUDE_BEFORE_KEY, before.getValue())
            .with(ScyllaConnectorConfig.CDC_INCLUDE_AFTER_KEY, after.getValue())
            .build());
  }

  @SuppressWarnings("unchecked")
  private void setUp(ScyllaConnectorConfig config, Optional<ChangeId> lastConsumed)
      throws InterruptedException {
    sourceInfo = new SourceInfo(config, TASK_ID);
    sourceInfo.setTaskState(windowState(lastConsumed));
    Map<TaskId, SourceInfo> sourceInfos = new HashMap<>();
    sourceInfos.put(TASK_ID, sourceInfo);
    offsetContext = new ScyllaOffsetContext(sourceInfos, new TransactionContext());
    dispatcher = Mockito.mock(EventDispatcher.class);
    Mockito.when(dispatcher.dispatchDataChangeEvent(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              snapshots.add(
                  new Snapshot(
                      currentRow,
                      sourceInfo.offset(),
                      sourceInfo.timestampUs(),
                      sourceInfo.timestamp()));
              return true;
            });
  }

  private static TaskState windowState(Optional<ChangeId> lastConsumed) {
    return new TaskState(
        new Timestamp(new Date(WINDOW_START_MS)),
        new Timestamp(new Date(WINDOW_END_MS)),
        lastConsumed);
  }

  private static ChangeId changeId(long ms) {
    return new ChangeId(STREAM, new ChangeTime(UUIDs.startOf(ms)));
  }

  private static RawChange row(
      OperationType op, long timeMs, Boolean endOfBatch, Integer ck, ChangeSchema schema) {
    RawChange change = Mockito.mock(RawChange.class);
    Mockito.when(change.getId()).thenReturn(changeId(timeMs));
    Mockito.when(change.getOperationType()).thenReturn(op);
    Mockito.when(change.isEndOfBatch()).thenReturn(Boolean.TRUE.equals(endOfBatch));
    Mockito.when(change.getSchema()).thenReturn(schema);
    Cell pk = Mockito.mock(Cell.class);
    Mockito.when(pk.getAsObject()).thenReturn(1);
    Cell ckCell = Mockito.mock(Cell.class);
    Mockito.when(ckCell.getAsObject()).thenReturn(ck);
    Mockito.when(change.getCell("pk")).thenReturn(pk);
    Mockito.when(change.getCell("ck")).thenReturn(ckCell);
    return change;
  }

  private static ChangeSchema schema() {
    ChangeSchema schema = Mockito.mock(ChangeSchema.class);
    ChangeSchema.ColumnDefinition pk = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(pk.getColumnName()).thenReturn("pk");
    Mockito.when(pk.getBaseTableColumnType()).thenReturn(ChangeSchema.ColumnType.PARTITION_KEY);
    ChangeSchema.ColumnDefinition ck = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(ck.getColumnName()).thenReturn("ck");
    Mockito.when(ck.getBaseTableColumnType()).thenReturn(ChangeSchema.ColumnType.CLUSTERING_KEY);
    Mockito.when(schema.getNonCdcColumnDefinitions()).thenReturn(List.of(pk, ck));
    return schema;
  }

  /** A table without clustering key, where a row delete arrives as PARTITION_DELETE. */
  private static ChangeSchema pkOnlySchema() {
    ChangeSchema schema = Mockito.mock(ChangeSchema.class);
    ChangeSchema.ColumnDefinition pk = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(pk.getColumnName()).thenReturn("pk");
    Mockito.when(pk.getBaseTableColumnType()).thenReturn(ChangeSchema.ColumnType.PARTITION_KEY);
    Mockito.when(schema.getNonCdcColumnDefinitions()).thenReturn(List.of(pk));
    return schema;
  }

  /**
   * Feeds rows the way scylla-cdc-java's {@code TaskAction} does: the task handed to the consumer
   * advances only on end-of-batch rows, and after each row the transport is told that state.
   */
  private void feed(
      TaskAndRawChangeConsumer consumer, Optional<ChangeId> lastConsumed, List<RawChange> rows) {
    Task task = new Task(TASK_ID, new TreeSet<>(List.of(STREAM)), windowState(lastConsumed));
    for (currentRow = 0; currentRow < rows.size(); currentRow++) {
      RawChange change = rows.get(currentRow);
      Task next = change.isEndOfBatch() ? task.updateState(change.getId()) : task;
      consumer.consume(task, change).join();
      offsetContext.taskStateOffsetContext(TASK_ID).dataChangeEvent(next.state);
      task = next;
    }
  }

  /** Restarting from any dispatched record's offset must re-read every row after it. */
  private void assertNoRowSkippedOnRestart(List<RawChange> rows) {
    assertFalse(snapshots.isEmpty(), "nothing was dispatched");
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> offset = new HashMap<>(snapshot.offset);
      Optional<ChangeId> resumeAfter =
          ScyllaOffsetContextLoader.deserializeTaskState(offset).getLastConsumedChangeId();
      for (int later = snapshot.rowIndex + 1; later < rows.size(); later++) {
        ChangeId id = rows.get(later).getId();
        // Driver3WorkerCQL keeps a row only if it is strictly after the saved ChangeId.
        assertTrue(
            resumeAfter.isEmpty() || id.compareTo(resumeAfter.get()) > 0,
            "restart from the offset of row " + snapshot.rowIndex + " would skip row " + later);
      }
    }
  }

  private void assertTimestampsAreTheRowsOwn(List<RawChange> rows) {
    for (Snapshot snapshot : snapshots) {
      ChangeTime changeTime = rows.get(snapshot.rowIndex).getId().getChangeTime();
      assertEquals(changeTime.getTimestamp(), snapshot.tsUs, "ts_us of row " + snapshot.rowIndex);
      assertEquals(
          changeTime.getDate().toInstant(), snapshot.tsMs, "ts_ms of row " + snapshot.rowIndex);
    }
  }

  private void runScenario(
      ScyllaConnectorConfig config,
      Optional<ChangeId> lastConsumed,
      Function<ScyllaConnectorConfig, TaskAndRawChangeConsumer> consumerFactory,
      List<RawChange> rows)
      throws InterruptedException {
    setUp(config, lastConsumed);
    feed(consumerFactory.apply(config), lastConsumed, rows);
    assertNoRowSkippedOnRestart(rows);
    assertTimestampsAreTheRowsOwn(rows);
  }

  private TaskAndRawChangeConsumer advanced(ScyllaConnectorConfig config) {
    return new ScyllaChangesConsumer(
        dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
  }

  private TaskAndRawChangeConsumer legacy(ScyllaConnectorConfig config) {
    return new ScyllaChangesConsumerLegacy(dispatcher, offsetContext, null, Clock.SYSTEM, config);
  }

  /** A static column written with clustered columns: static row first, clustered row last. */
  private static List<RawChange> staticSplitBatch() {
    ChangeSchema schema = schema();
    return List.of(
        row(OperationType.ROW_INSERT, 11_000, true, 0, schema),
        row(OperationType.ROW_UPDATE, 12_000, null, null, schema),
        row(OperationType.ROW_UPDATE, 12_000, true, 0, schema));
  }

  @Test
  void noImages_midBatchRecordDoesNotCarryTheBatchOffset() throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        staticSplitBatch());
    assertEquals(3, snapshots.size());
  }

  @Test
  void legacy_midBatchRecordDoesNotCarryTheBatchOffset() throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::legacy,
        staticSplitBatch());
    assertEquals(3, snapshots.size());
  }

  /** Guards the correction to #340: a row that is not dispatched never reaches Kafka's offset. */
  @Test
  void ignoredPreImageRow_neverCarriesAnOffsetIntoKafka() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_INSERT, 11_000, true, 0, schema),
            row(OperationType.PRE_IMAGE, 12_000, null, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, true, 0, schema)));
    assertEquals(2, snapshots.size());
  }

  /**
   * With table postimages on but not consumed, the end-of-batch row is never dispatched, so the
   * batch's own offset never reaches Kafka and a restart re-emits the batch: duplicates, not loss.
   */
  @Test
  void ignoredPostImageAtEndOfBatch_replaysTheBatchOnRestart() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_INSERT, 11_000, true, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, 0, schema),
            row(OperationType.POST_IMAGE, 12_000, true, 0, schema)));
    assertEquals(2, snapshots.size());
    assertEquals(
        changeId(11_000).getChangeTime().getUUID().toString(),
        snapshots.get(1).offset.get(SourceInfo.CHANGE_ID_TIME));
  }

  @Test
  void postImages_staticRowCompletingMidBatchDoesNotCarryTheBatchOffset()
      throws InterruptedException {
    ChangeSchema schema = schema();
    // The batch shape ScyllaDB emits with preimage and postimage on (see #341).
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.FULL),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.PRE_IMAGE, 12_000, null, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, null, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, 0, schema),
            row(OperationType.POST_IMAGE, 12_000, null, null, schema),
            row(OperationType.POST_IMAGE, 12_000, true, 0, schema)));
    assertEquals(2, snapshots.size());
    assertEquals(3, snapshots.get(0).rowIndex);
  }

  /**
   * Deleting a missing partition writes no PRE_IMAGE (#368). Both deletes must be dispatched; a
   * task left waiting would never complete with this consumer's infinite timeout.
   */
  private void assertPartitionDeletesDispatched(ScyllaConnectorConfig config)
      throws InterruptedException {
    ChangeSchema schema = pkOnlySchema();
    runScenario(
        config,
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.PARTITION_DELETE, 11_000, true, null, schema),
            row(OperationType.PRE_IMAGE, 12_000, null, null, schema),
            row(OperationType.PARTITION_DELETE, 12_000, true, null, schema)));
    assertEquals(2, snapshots.size());
    assertEquals(0, snapshots.get(0).rowIndex);
    assertEquals(2, snapshots.get(1).rowIndex);
  }

  @Test
  void preImages_partitionDeleteWithOrWithoutPreimageIsDispatched() throws InterruptedException {
    assertPartitionDeletesDispatched(config(CdcIncludeMode.FULL, CdcIncludeMode.NONE));
  }

  @Test
  void preAndPostImages_partitionDeleteWithOrWithoutPreimageIsDispatched()
      throws InterruptedException {
    assertPartitionDeletesDispatched(config(CdcIncludeMode.FULL, CdcIncludeMode.FULL));
  }

  @Test
  void freshWindow_midBatchRecordHasTimestampAndNoChangeIdInOffset() throws InterruptedException {
    ChangeSchema schema = schema();
    List<RawChange> rows =
        List.of(
            row(OperationType.ROW_UPDATE, 12_000, null, null, schema),
            row(OperationType.ROW_UPDATE, 12_000, true, 0, schema));
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE), Optional.empty(), this::advanced, rows);
    assertFalse(snapshots.get(0).offset.containsKey(SourceInfo.CHANGE_ID_TIME));
    assertEquals(
        rows.get(1).getId().getChangeTime().getUUID().toString(),
        snapshots.get(1).offset.get(SourceInfo.CHANGE_ID_TIME));
  }
}
