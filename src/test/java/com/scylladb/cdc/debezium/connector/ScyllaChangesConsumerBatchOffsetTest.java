package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

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
  private final List<Envelope.Operation> operations = new ArrayList<>();
  private final List<TaskInfo> records = new ArrayList<>();
  private int currentRow;

  /** Thrown by a dispatch, if set, once {@link #dispatchesBeforeFailure} dispatches succeed. */
  private RuntimeException nextDispatchFailure;

  private int dispatchesBeforeFailure;

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
              if (nextDispatchFailure != null && dispatchesBeforeFailure-- == 0) {
                RuntimeException failure = nextDispatchFailure;
                nextDispatchFailure = null;
                throw failure;
              }
              Object emitter = invocation.getArgument(2);
              if (emitter instanceof ScyllaChangeRecordEmitter) {
                operations.add(((ScyllaChangeRecordEmitter) emitter).getOperation());
                records.add(((ScyllaChangeRecordEmitter) emitter).getTaskInfo());
              } else if (emitter instanceof ScyllaChangeRecordEmitterLegacy) {
                operations.add(((ScyllaChangeRecordEmitterLegacy) emitter).getOperation());
              }
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
      OperationType op,
      long timeMs,
      Boolean endOfBatch,
      int batchSeqNo,
      Integer ck,
      ChangeSchema schema) {
    RawChange change = Mockito.mock(RawChange.class);
    Mockito.when(change.getId()).thenReturn(changeId(timeMs));
    Mockito.when(change.getOperationType()).thenReturn(op);
    Mockito.when(change.isEndOfBatch()).thenReturn(Boolean.TRUE.equals(endOfBatch));
    Mockito.when(change.getBatchSequenceNumber()).thenReturn(batchSeqNo);
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

  /** A consumer whose batches are all overdue for {@code cdc.incomplete.task.timeout.ms}. */
  private TaskAndRawChangeConsumer advancedWithOverdueBatches(ScyllaConnectorConfig config) {
    return new ScyllaChangesConsumer(
        dispatcher, offsetContext, null, Clock.SYSTEM, config, null, -1);
  }

  private static String uuid(long ms) {
    return changeId(ms).getChangeTime().getUUID().toString();
  }

  /** The ChangeId time each dispatched record carries in its offset, in dispatch order. */
  private List<String> offsets() {
    List<String> offsets = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      offsets.add(snapshot.offset.get(SourceInfo.CHANGE_ID_TIME));
    }
    return offsets;
  }

  /** Forgets what earlier dispatches recorded, for a test running several scenarios. */
  private void clearDispatched() {
    snapshots.clear();
    operations.clear();
    records.clear();
  }

  /** The clustering key of each dispatched record, in dispatch order. */
  private List<Object> cks() {
    List<Object> cks = new ArrayList<>();
    for (TaskInfo record : records) {
      cks.add(record.getChange().getCell("ck").getAsObject());
    }
    return cks;
  }

  private TaskAndRawChangeConsumer legacy(ScyllaConnectorConfig config) {
    return new ScyllaChangesConsumerLegacy(dispatcher, offsetContext, null, Clock.SYSTEM, config);
  }

  /** A static column written with clustered columns: static row first, clustered row last. */
  private static List<RawChange> staticSplitBatch() {
    ChangeSchema schema = schema();
    return List.of(
        row(OperationType.ROW_INSERT, 11_000, true, 0, 0, schema),
        row(OperationType.ROW_UPDATE, 12_000, null, 0, null, schema),
        row(OperationType.ROW_UPDATE, 12_000, true, 1, 0, schema));
  }

  @Test
  void noImages_midBatchRecordDoesNotCarryTheBatchOffset() throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        staticSplitBatch());
    // The second batch's first record keeps the previous batch's offset; its last carries its own.
    assertEquals(List.of(uuid(11_000), uuid(11_000), uuid(12_000)), offsets());
  }

  @Test
  void legacy_midBatchRecordDoesNotCarryTheBatchOffset() throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::legacy,
        staticSplitBatch());
    assertEquals(List.of(uuid(11_000), uuid(11_000), uuid(12_000)), offsets());
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
            row(OperationType.ROW_INSERT, 11_000, true, 0, 0, schema),
            row(OperationType.PRE_IMAGE, 12_000, null, 0, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, true, 1, 0, schema)));
    assertEquals(2, snapshots.size());
  }

  /**
   * With table postimages on but not consumed, the batch's record is dispatched at the end-of-batch
   * row, so it carries the batch's own offset.
   */
  @Test
  void ignoredPostImageAtEndOfBatch_recordCarriesTheBatchOffset() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_INSERT, 11_000, true, 0, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, 0, 0, schema),
            row(OperationType.POST_IMAGE, 12_000, true, 1, 0, schema)));
    assertEquals(2, snapshots.size());
    assertEquals(
        changeId(12_000).getChangeTime().getUUID().toString(),
        snapshots.get(1).offset.get(SourceInfo.CHANGE_ID_TIME));
  }

  /**
   * Preimages consumed, postimages not, on a table logging both (#363): the ignored postimage row
   * ends its batch and leaves no state, so the next write to the row gets its own preimage.
   */
  @Test
  void ignoredPostImages_eachWriteKeepsItsOwnPreimage() throws Exception {
    for (CdcIncludeMode before : List.of(CdcIncludeMode.FULL, CdcIncludeMode.ONLY_UPDATED)) {
      clearDispatched();
      ChangeSchema schema = schema();
      List<RawChange> rows =
          List.of(
              row(OperationType.PRE_IMAGE, 12_000, null, 0, 0, schema),
              row(OperationType.ROW_UPDATE, 12_000, null, 1, 0, schema),
              row(OperationType.POST_IMAGE, 12_000, true, 2, 0, schema),
              row(OperationType.PRE_IMAGE, 13_000, null, 0, 0, schema),
              row(OperationType.ROW_UPDATE, 13_000, null, 1, 0, schema),
              row(OperationType.POST_IMAGE, 13_000, true, 2, 0, schema));
      ScyllaConnectorConfig config = config(before, CdcIncludeMode.NONE);
      setUp(config, Optional.of(changeId(10_500)));
      ScyllaChangesConsumer consumer =
          new ScyllaChangesConsumer(
              dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
      // The test classpath has no SLF4J binding, so a mock is the only way to see the warnings.
      Logger logger = Mockito.mock(Logger.class);
      Field loggerField = ScyllaChangesConsumer.class.getDeclaredField("logger");
      loggerField.setAccessible(true);
      loggerField.set(consumer, logger);

      feed(consumer, Optional.of(changeId(10_500)), rows);

      assertNoRowSkippedOnRestart(rows);
      assertTimestampsAreTheRowsOwn(rows);
      assertEquals(
          List.of(Envelope.Operation.UPDATE, Envelope.Operation.UPDATE), operations, "" + before);
      // Each write goes out at its own postimage row, with its own offset, change and preimage.
      assertEquals(
          List.of(2, 5),
          List.of(snapshots.get(0).rowIndex, snapshots.get(1).rowIndex),
          "" + before);
      assertEquals(List.of(uuid(12_000), uuid(13_000)), offsets(), "" + before);
      assertSame(rows.get(1), records.get(0).getChange(), "" + before);
      assertSame(rows.get(0), records.get(0).getPreImage(), "" + before);
      assertSame(rows.get(4), records.get(1).getChange(), "" + before);
      assertSame(rows.get(3), records.get(1).getPreImage(), "" + before);
      // A batch still pending would be dispatched, with a warning, at the window end.
      consumer.windowEnded(TASK_ID);
      assertEquals(2, operations.size(), "" + before);
      assertTrue(
          Mockito.mockingDetails(logger).getInvocations().stream()
              .map(invocation -> invocation.getMethod().getName())
              .noneMatch(name -> name.equals("warn") || name.equals("error")),
          "" + before);
    }
  }

  @Test
  void postImages_staticSplitBatch_dispatchesBothRecordsAtEndOfBatch() throws InterruptedException {
    ChangeSchema schema = schema();
    // The batch shape ScyllaDB emits with preimage and postimage on (see #341).
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.FULL),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.PRE_IMAGE, 12_000, null, 0, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, 1, null, schema),
            row(OperationType.ROW_UPDATE, 12_000, null, 2, 0, schema),
            row(OperationType.POST_IMAGE, 12_000, null, 3, null, schema),
            row(OperationType.POST_IMAGE, 12_000, true, 4, 0, schema)));
    // Both records go out at the end-of-batch row; only the last carries the batch's offset.
    assertEquals(2, snapshots.size());
    assertEquals(4, snapshots.get(0).rowIndex);
    assertEquals(List.of(uuid(10_500), uuid(12_000)), offsets());
  }

  /**
   * An upsert and a delete of one row in one batch: ScyllaDB logs the upsert first, then the
   * deletion, and no postimage after it. The row yields the upsert record, then the delete.
   */
  private void assertUpsertThenDelete(
      CdcIncludeMode before, CdcIncludeMode after, OperationType upsert, Envelope.Operation first)
      throws InterruptedException {
    ChangeSchema schema = schema();
    List<RawChange> rows = new ArrayList<>();
    if (upsert == OperationType.ROW_UPDATE) {
      rows.add(row(OperationType.PRE_IMAGE, 12_000, null, 0, 0, schema));
    }
    rows.add(row(upsert, 12_000, null, rows.size(), 0, schema));
    rows.add(row(OperationType.ROW_DELETE, 12_000, true, rows.size(), 0, schema));
    runScenario(config(before, after), Optional.of(changeId(10_500)), this::advanced, rows);
    assertEquals(List.of(first, Envelope.Operation.DELETE), operations);
    assertEquals(List.of(uuid(10_500), uuid(12_000)), offsets());
  }

  @Test
  void updateThenDelete_emitsBothRecordsInEveryMode() throws InterruptedException {
    for (CdcIncludeMode before : CdcIncludeMode.values()) {
      for (CdcIncludeMode after : CdcIncludeMode.values()) {
        clearDispatched();
        assertUpsertThenDelete(before, after, OperationType.ROW_UPDATE, Envelope.Operation.UPDATE);
      }
    }
  }

  @Test
  void insertThenDelete_emitsBothRecordsInEveryMode() throws InterruptedException {
    for (CdcIncludeMode before : CdcIncludeMode.values()) {
      for (CdcIncludeMode after : CdcIncludeMode.values()) {
        clearDispatched();
        assertUpsertThenDelete(before, after, OperationType.ROW_INSERT, Envelope.Operation.CREATE);
      }
    }
  }

  /**
   * A batch whose rows outlast {@code cdc.incomplete.task.timeout.ms} still waits for its last row:
   * a slow read is not a missing row.
   */
  @Test
  void overdueBatch_stillWaitsForItsLastRow() throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
        Optional.of(changeId(10_500)),
        this::advancedWithOverdueBatches,
        ttlInsertWithCollectionBatch());
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    assertEquals(List.of(uuid(12_000)), offsets());
  }

  /** A batch with no end-of-batch row is dispatched, with its offset, when the next one starts. */
  @Test
  void batchWithoutEndOfBatchRow_isDispatchedWhenTheNextBatchStarts() throws InterruptedException {
    ChangeSchema schema = schema();
    List<RawChange> rows =
        List.of(
            row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema),
            row(OperationType.ROW_INSERT, 13_000, true, 0, 1, schema));
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    feed(advanced(config), Optional.of(changeId(10_500)), rows);
    assertNoRowSkippedOnRestart(rows);
    assertEquals(List.of(Envelope.Operation.CREATE, Envelope.Operation.CREATE), operations);
    assertEquals(List.of(uuid(12_000), uuid(13_000)), offsets());
    // The record is stamped with its own row's time, not the row that triggered the dispatch.
    assertEquals(changeId(12_000).getChangeTime().getTimestamp(), snapshots.get(0).tsUs);
  }

  /**
   * A batch still pending when its window ends is dispatched before the offset moves to the next
   * window, so that checkpoint never skips it.
   */
  @Test
  void batchPendingAtWindowEnd_isDispatchedBeforeTheWindowMoves() throws InterruptedException {
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
    feed(
        consumer,
        Optional.of(changeId(10_500)),
        List.of(row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema())));
    assertTrue(snapshots.isEmpty());

    TaskState nextWindow =
        new TaskState(
            new Timestamp(new Date(WINDOW_END_MS)),
            new Timestamp(new Date(WINDOW_END_MS + 10_000)),
            Optional.empty());
    new ScyllaWorkerTransport(null, offsetContext, dispatcher, Long.MAX_VALUE, consumer)
        .moveStateToNextWindow(TASK_ID, nextWindow);
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    // Dispatched under the batch's offset, before the window's state replaced it.
    assertEquals(List.of(uuid(12_000)), offsets());
  }

  /** The library retries a failed window move without re-reading the window. */
  @Test
  void failedWindowEndDispatch_isRetriedWithTheWindowMove() throws InterruptedException {
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
    feed(
        consumer,
        Optional.of(changeId(10_500)),
        List.of(row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema())));

    TaskState nextWindow =
        new TaskState(
            new Timestamp(new Date(WINDOW_END_MS)),
            new Timestamp(new Date(WINDOW_END_MS + 10_000)),
            Optional.empty());
    ScyllaWorkerTransport transport =
        new ScyllaWorkerTransport(null, offsetContext, dispatcher, Long.MAX_VALUE, consumer);
    nextDispatchFailure = new IllegalStateException("queue failure");
    assertThrows(
        IllegalStateException.class, () -> transport.moveStateToNextWindow(TASK_ID, nextWindow));
    assertTrue(operations.isEmpty());

    transport.moveStateToNextWindow(TASK_ID, nextWindow);
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    assertEquals(List.of(uuid(12_000)), offsets());
  }

  /** A window ending with nothing buffered moves the checkpoint and dispatches nothing. */
  @Test
  void windowEndWithoutPendingBatch_movesTheWindow() throws InterruptedException {
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
    String windowStart = sourceInfo.offset().get(SourceInfo.WINDOW_START);
    TaskState nextWindow =
        new TaskState(
            new Timestamp(new Date(WINDOW_END_MS)),
            new Timestamp(new Date(WINDOW_END_MS + 10_000)),
            Optional.empty());
    new ScyllaWorkerTransport(null, offsetContext, dispatcher, Long.MAX_VALUE, consumer)
        .moveStateToNextWindow(TASK_ID, nextWindow);
    assertTrue(operations.isEmpty());
    assertTrue(!windowStart.equals(sourceInfo.offset().get(SourceInfo.WINDOW_START)));
  }

  /** A batch of images only has no record; the next batch is dispatched as usual. */
  @Test
  void imagesOnlyBatch_dispatchesNothing() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.PRE_IMAGE, 11_000, null, 0, 0, schema),
            row(OperationType.POST_IMAGE, 11_000, true, 1, 0, schema),
            row(OperationType.ROW_INSERT, 12_000, true, 0, 0, schema)));
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    assertEquals(List.of(uuid(12_000)), offsets());
  }

  /**
   * A clustered table's partition delete is not supported and skipped; ending the batch, it still
   * dispatches the batch's update with the batch's offset.
   */
  @Test
  void skippedPartitionDeleteAtEndOfBatch_dispatchesTheBatch() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_UPDATE, 12_000, null, 0, 0, schema),
            row(OperationType.PARTITION_DELETE, 12_000, true, 1, null, schema)));
    assertEquals(List.of(Envelope.Operation.UPDATE), operations);
    assertEquals(List.of(uuid(12_000)), offsets());
  }

  /**
   * A window move failing after part of the batch went out: the retry sends the whole batch again,
   * and the window checkpoint and heartbeat wait for it.
   */
  @Test
  void windowEndDispatchFailingMidBatch_isRetriedInFull() throws InterruptedException {
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, null, Clock.SYSTEM, config, null, Long.MAX_VALUE);
    ChangeSchema schema = schema();
    feed(
        consumer,
        Optional.of(changeId(10_500)),
        List.of(
            row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema),
            row(OperationType.ROW_INSERT, 12_000, null, 1, 1, schema)));
    String windowStart = sourceInfo.offset().get(SourceInfo.WINDOW_START);

    TaskState nextWindow =
        new TaskState(
            new Timestamp(new Date(WINDOW_END_MS)),
            new Timestamp(new Date(WINDOW_END_MS + 10_000)),
            Optional.empty());
    // A heartbeat timer that has always expired: every completed window move sends one.
    ScyllaWorkerTransport transport =
        new ScyllaWorkerTransport(null, offsetContext, dispatcher, -1, consumer);
    nextDispatchFailure = new IllegalStateException("queue failure");
    dispatchesBeforeFailure = 1;
    assertThrows(
        IllegalStateException.class, () -> transport.moveStateToNextWindow(TASK_ID, nextWindow));
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    assertEquals(Arrays.asList(0), cks());
    assertEquals(windowStart, sourceInfo.offset().get(SourceInfo.WINDOW_START));
    Mockito.verify(dispatcher, Mockito.never()).alwaysDispatchHeartbeatEvent(any(), any());

    transport.moveStateToNextWindow(TASK_ID, nextWindow);
    assertEquals(
        List.of(Envelope.Operation.CREATE, Envelope.Operation.CREATE, Envelope.Operation.CREATE),
        operations);
    // The first record went out twice (at least once); only the batch's last carries its offset.
    assertEquals(Arrays.asList(0, 0, 1), cks());
    assertEquals(List.of(uuid(10_500), uuid(10_500), uuid(12_000)), offsets());
    assertTrue(
        !windowStart.equals(sourceInfo.offset().get(SourceInfo.WINDOW_START)),
        "the window checkpoint moves once the batch is out");
    Mockito.verify(dispatcher).alwaysDispatchHeartbeatEvent(any(), any());
  }

  /**
   * After a failure the library re-reads the window from the last saved offset, so the pending
   * batch's rows come again from its first one. They replace the buffered rows rather than join
   * them.
   */
  @Test
  void reReadBatch_replacesItsBufferedRows() throws InterruptedException {
    ChangeSchema schema = schema();
    RawChange upsert = row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema);
    RawChange delete = row(OperationType.ROW_DELETE, 12_000, null, 1, 0, schema);
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            upsert,
            delete,
            upsert,
            delete,
            row(OperationType.ROW_DELETE, 12_000, true, 2, 1, schema)));
    assertEquals(
        List.of(Envelope.Operation.CREATE, Envelope.Operation.DELETE, Envelope.Operation.DELETE),
        operations);
  }

  /**
   * A batch without an end-of-batch row does not move the library's state, so a failure in the next
   * batch re-reads from before it. Its row arriving while the next batch is pending is a re-read,
   * not the end of the pending batch.
   */
  @Test
  void reReadFromAnEarlierBatch_dropsThePendingOne() throws InterruptedException {
    ChangeSchema schema = schema();
    RawChange first = row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema);
    RawChange secondStart = row(OperationType.ROW_INSERT, 13_000, null, 0, 0, schema);
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    setUp(config, Optional.of(changeId(10_500)));
    feed(
        advanced(config),
        Optional.of(changeId(10_500)),
        List.of(
            first,
            secondStart,
            first,
            secondStart,
            row(OperationType.ROW_INSERT, 13_000, true, 1, 1, schema)));
    // The first batch twice (at-least-once), then the whole second batch once.
    assertEquals(4, operations.size());
    assertEquals(List.of(uuid(12_000), uuid(12_000), uuid(10_500), uuid(13_000)), offsets());
  }

  /**
   * Two additions to one set with different TTLs in one batch: ScyllaDB logs two deltas, each with
   * only its own elements, so merging them would lose the first.
   */
  @Test
  void collectionAdditionsWithDifferentTtls_emitOneRecordEach() throws InterruptedException {
    ChangeSchema schema = schema();
    ChangeSchema.ColumnDefinition set = regularColumn("s");
    Mockito.when(schema.getAllColumnDefinitions()).thenReturn(List.of(set));
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_UPDATE, 12_000, null, 0, 0, schema),
            row(OperationType.ROW_UPDATE, 12_000, true, 1, 0, schema)));
    assertEquals(List.of(Envelope.Operation.UPDATE, Envelope.Operation.UPDATE), operations);
    assertEquals(List.of(uuid(10_500), uuid(12_000)), offsets());
  }

  /**
   * The postimage is the row after the whole batch, so both records of the split row carry it: an
   * upsert without one would have after=null although the row exists.
   */
  @Test
  void collectionAdditionsWithDifferentTtls_everyRecordCarriesThePostimage()
      throws InterruptedException {
    for (CdcIncludeMode after : CdcIncludeMode.values()) {
      clearDispatched();
      ChangeSchema schema = schema();
      ChangeSchema.ColumnDefinition set = regularColumn("s");
      Mockito.when(schema.getAllColumnDefinitions()).thenReturn(List.of(set));
      RawChange postImage = row(OperationType.POST_IMAGE, 12_000, true, 2, 0, schema);
      runScenario(
          config(CdcIncludeMode.NONE, after),
          Optional.of(changeId(10_500)),
          this::advanced,
          List.of(
              row(OperationType.ROW_UPDATE, 12_000, null, 0, 0, schema),
              row(OperationType.ROW_UPDATE, 12_000, null, 1, 0, schema),
              postImage));
      assertEquals(
          List.of(Envelope.Operation.UPDATE, Envelope.Operation.UPDATE), operations, after.name());
      RawChange expected = after.requiresImage() ? postImage : null;
      for (TaskInfo record : records) {
        assertSame(expected, record.getPostImage(), after.name());
      }
    }
  }

  /** A delta or image of row (1, 0) in a real schema, with {@code values} set and the rest null. */
  private static RawChange valuedRow(
      OperationType op,
      int batchSeqNo,
      boolean endOfBatch,
      ChangeSchema schema,
      Map<String, Object> values) {
    return valuedRow(op, batchSeqNo, endOfBatch, schema, 0, values);
  }

  /** A delta or image of row (1, {@code ck}); a null {@code ck} is the static row. */
  private static RawChange valuedRow(
      OperationType op,
      int batchSeqNo,
      boolean endOfBatch,
      ChangeSchema schema,
      Integer ck,
      Map<String, Object> values) {
    RawChange change = Mockito.mock(RawChange.class);
    Mockito.when(change.getId()).thenReturn(changeId(12_000));
    Mockito.when(change.getOperationType()).thenReturn(op);
    Mockito.when(change.isEndOfBatch()).thenReturn(endOfBatch);
    Mockito.when(change.getBatchSequenceNumber()).thenReturn(batchSeqNo);
    Mockito.when(change.getSchema()).thenReturn(schema);
    Map<String, Object> all = new HashMap<>(values);
    all.put("pk", 1);
    all.put("ck", ck);
    for (ChangeSchema.ColumnDefinition cdef : schema.getAllColumnDefinitions()) {
      Object value = all.get(cdef.getColumnName());
      Cell cell = Mockito.mock(Cell.class);
      Mockito.when(cell.getAsObject()).thenReturn(value);
      Mockito.when(cell.getDataType()).thenReturn(cdef.getCdcLogDataType());
      Mockito.when(change.getCell(cdef.getColumnName())).thenReturn(cell);
      Mockito.when(change.isNull(cdef)).thenReturn(value == null);
    }
    return change;
  }

  /** A real schema: partition key pk, clustering key ck, and text columns s and t. */
  private static ChangeSchema realSchema() {
    ChangeSchema.DataType integer = new ChangeSchema.DataType(ChangeSchema.CqlType.INT);
    ChangeSchema.DataType text = new ChangeSchema.DataType(ChangeSchema.CqlType.TEXT);
    return new ChangeSchema(
        List.of(
            new ChangeSchema.ColumnDefinition(
                "pk", 0, integer, integer, ChangeSchema.ColumnKind.PARTITION_KEY),
            new ChangeSchema.ColumnDefinition(
                "ck", 1, integer, integer, ChangeSchema.ColumnKind.CLUSTERING_KEY),
            new ChangeSchema.ColumnDefinition("s", 2, text, text, ChangeSchema.ColumnKind.REGULAR),
            new ChangeSchema.ColumnDefinition(
                "t", 3, text, text, ChangeSchema.ColumnKind.REGULAR)));
  }

  /**
   * Feeds {@code rows}, checks the offsets as {@link #runScenario} does, and returns the envelope
   * of every record the emitters build.
   */
  private List<Struct> emitEnvelopes(
      ScyllaConnectorConfig config, ChangeSchema changeSchema, List<RawChange> rows)
      throws InterruptedException {
    setUp(config, Optional.of(changeId(10_500)));
    ScyllaSchema scyllaSchema =
        new ScyllaSchema(config, config.getSourceInfoStructMaker().schema());
    ScyllaCollectionSchema collectionSchema =
        scyllaSchema.updateChangeSchema(new CollectionId(TABLE), changeSchema);
    List<Struct> envelopes = new ArrayList<>();
    // doAnswer: re-stubbing with when() would call setUp's answer and record a stray snapshot.
    Mockito.doAnswer(
            invocation -> {
              ScyllaChangeRecordEmitter emitter = invocation.getArgument(2);
              emitter.emitChangeRecords(
                  collectionSchema,
                  (partition, schema, operation, key, value, offset, headers) ->
                      envelopes.add(value));
              snapshots.add(
                  new Snapshot(
                      currentRow,
                      sourceInfo.offset(),
                      sourceInfo.timestampUs(),
                      sourceInfo.timestamp()));
              return true;
            })
        .when(dispatcher)
        .dispatchDataChangeEvent(any(), any(), any());
    ScyllaChangesConsumer consumer =
        new ScyllaChangesConsumer(
            dispatcher, offsetContext, scyllaSchema, Clock.SYSTEM, config, null, Long.MAX_VALUE);
    feed(consumer, Optional.of(changeId(10_500)), rows);
    assertNoRowSkippedOnRestart(rows);
    assertTimestampsAreTheRowsOwn(rows);
    return envelopes;
  }

  /**
   * A row the batch splits into two records, the second also setting {@code t}: both records show
   * the row before the batch, so neither takes {@code t} from the postimage.
   */
  @Test
  void splitRow_bothRecordsCarryTheRowBeforeTheBatch() throws InterruptedException {
    ChangeSchema changeSchema = realSchema();
    List<Struct> envelopes =
        emitEnvelopes(
            config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
            changeSchema,
            List.of(
                valuedRow(
                    OperationType.PRE_IMAGE,
                    0,
                    false,
                    changeSchema,
                    Map.of("s", "old-s", "t", "old-t")),
                valuedRow(OperationType.ROW_UPDATE, 1, false, changeSchema, Map.of("s", "s1")),
                valuedRow(
                    OperationType.ROW_UPDATE,
                    2,
                    false,
                    changeSchema,
                    Map.of("s", "s2", "t", "new-t")),
                valuedRow(
                    OperationType.POST_IMAGE,
                    3,
                    true,
                    changeSchema,
                    Map.of("s", "s2", "t", "new-t"))));
    assertEquals(2, envelopes.size());
    for (Struct envelope : envelopes) {
      Struct before = envelope.getStruct(Envelope.FieldName.BEFORE);
      assertEquals("old-s", before.get("s"));
      assertEquals("old-t", before.get("t"));
    }
  }

  /** An ordinary update reads its changed column once, however many images it is built with. */
  @Test
  void update_bothImages_readsItsChangedColumnOnce() throws InterruptedException {
    ChangeSchema schema = realSchema();
    RawChange delta = valuedRow(OperationType.ROW_UPDATE, 1, false, schema, Map.of("s", "s1"));
    emitEnvelopes(
        config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
        schema,
        List.of(
            valuedRow(OperationType.PRE_IMAGE, 0, false, schema, Map.of("s", "old")),
            delta,
            valuedRow(OperationType.POST_IMAGE, 2, true, schema, Map.of("s", "s1"))));
    long reads =
        Mockito.mockingDetails(delta.getCell("s")).getInvocations().stream()
            .filter(invocation -> invocation.getMethod().getName().equals("getAsObject"))
            .count();
    assertEquals(1, reads);
  }

  /**
   * Every delta of a row is read a bounded number of times, however many records the row splits
   * into: a full {@code before} needs the columns all of them modify, built once per row.
   */
  @Test
  void splitRow_readsEachDeltaABoundedNumberOfTimes() throws InterruptedException {
    ChangeSchema changeSchema = realSchema();
    List<RawChange> deltas = new ArrayList<>();
    List<RawChange> rows = new ArrayList<>();
    rows.add(valuedRow(OperationType.PRE_IMAGE, 0, false, changeSchema, Map.of("s", "old")));
    int deltaCount = 40;
    for (int i = 1; i <= deltaCount; i++) {
      // Each delta sets s again, so each is its own record.
      RawChange delta =
          valuedRow(OperationType.ROW_UPDATE, i, false, changeSchema, Map.of("s", "s" + i));
      deltas.add(delta);
      rows.add(delta);
    }
    rows.add(
        valuedRow(
            OperationType.POST_IMAGE, deltaCount + 1, true, changeSchema, Map.of("s", "new")));
    List<Struct> envelopes =
        emitEnvelopes(config(CdcIncludeMode.FULL, CdcIncludeMode.FULL), changeSchema, rows);
    assertEquals(deltaCount, envelopes.size());
    long cellReads = 0;
    for (RawChange delta : deltas) {
      cellReads +=
          Mockito.mockingDetails(delta).getInvocations().stream()
              .filter(invocation -> invocation.getMethod().getName().equals("getCell"))
              .count();
    }
    // 6 per delta: 2 for its row key, 4 for its modified columns. Scanning it twice reads 10;
    // rebuilding the row's columns for each record read 166.
    assertTrue(
        cellReads < 8L * deltaCount, cellReads + " cell reads for " + deltaCount + " deltas");
  }

  /**
   * {@code INSERT ... USING TTL} with a non-frozen collection: ScyllaDB logs the INSERT, the
   * collection tombstone and the collection cells as separate deltas of one batch (#365).
   */
  private static List<RawChange> ttlInsertWithCollectionBatch() {
    ChangeSchema schema = schema();
    ChangeSchema.ColumnDefinition t = regularColumn("t");
    ChangeSchema.ColumnDefinition l = regularColumn("l");
    ChangeSchema.ColumnDefinition deletedL = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(deletedL.getColumnName()).thenReturn("cdc$deleted_l");
    Mockito.when(deletedL.isCdcColumn()).thenReturn(true);
    Mockito.when(schema.getAllColumnDefinitions()).thenReturn(List.of(t, l, deletedL));
    return List.of(
        setting(row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema), t),
        setting(row(OperationType.ROW_UPDATE, 12_000, null, 1, 0, schema), deletedL),
        setting(row(OperationType.ROW_UPDATE, 12_000, null, 2, 0, schema), l),
        row(OperationType.POST_IMAGE, 12_000, true, 3, 0, schema));
  }

  private static ChangeSchema.ColumnDefinition regularColumn(String name) {
    ChangeSchema.ColumnDefinition cdef = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(cdef.getColumnName()).thenReturn(name);
    Mockito.when(cdef.getBaseTableColumnKind()).thenReturn(ChangeSchema.ColumnKind.REGULAR);
    return cdef;
  }

  /** {@code change}, with {@code set} its only non-null column. */
  private static RawChange setting(RawChange change, ChangeSchema.ColumnDefinition set) {
    Mockito.when(change.isNull(any(ChangeSchema.ColumnDefinition.class)))
        .thenAnswer(invocation -> invocation.getArgument(0) != set);
    return change;
  }

  private void assertOneCreateForTtlInsertWithCollection(
      CdcIncludeMode before, CdcIncludeMode after) throws InterruptedException {
    List<RawChange> rows = ttlInsertWithCollectionBatch();
    runScenario(config(before, after), Optional.of(changeId(10_500)), this::advanced, rows);
    assertEquals(List.of(Envelope.Operation.CREATE), operations);
    assertEquals(
        changeId(12_000).getChangeTime().getUUID().toString(),
        snapshots.get(0).offset.get(SourceInfo.CHANGE_ID_TIME));
  }

  @Test
  void ttlInsertWithCollection_noImages_emitsOneCreate() throws InterruptedException {
    assertOneCreateForTtlInsertWithCollection(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
  }

  @Test
  void ttlInsertWithCollection_beforeImage_emitsOneCreate() throws InterruptedException {
    assertOneCreateForTtlInsertWithCollection(CdcIncludeMode.FULL, CdcIncludeMode.NONE);
  }

  @Test
  void ttlInsertWithCollection_afterImage_emitsOneCreate() throws InterruptedException {
    assertOneCreateForTtlInsertWithCollection(CdcIncludeMode.NONE, CdcIncludeMode.FULL);
  }

  @Test
  void ttlInsertWithCollection_bothImages_emitsOneCreate() throws InterruptedException {
    assertOneCreateForTtlInsertWithCollection(CdcIncludeMode.FULL, CdcIncludeMode.FULL);
  }

  /** An UPDATE of a missing row gets no preimage; the batch end still dispatches it. */
  @Test
  void updateWithoutPreimage_bothImages_emitsBeforeNull() throws InterruptedException {
    ChangeSchema schema = realSchema();
    List<Struct> envelopes =
        emitEnvelopes(
            config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
            schema,
            List.of(
                valuedRow(OperationType.ROW_UPDATE, 0, false, schema, Map.of("s", "s1")),
                valuedRow(OperationType.POST_IMAGE, 1, true, schema, Map.of("s", "s1"))));
    assertEquals(1, envelopes.size());
    Struct envelope = envelopes.get(0);
    assertEquals(
        Envelope.Operation.UPDATE.code(), envelope.getString(Envelope.FieldName.OPERATION));
    assertNull(envelope.get(Envelope.FieldName.BEFORE));
    assertEquals("s1", envelope.getStruct(Envelope.FieldName.AFTER).get("s"));
  }

  /**
   * A static column written with clustered columns, with both images (#341): the static row has no
   * preimage, so its record has before=null, while the clustered row keeps its own before.
   */
  @Test
  void staticSplit_bothImages_onlyTheStaticRecordHasNoBefore() throws InterruptedException {
    ChangeSchema schema = realSchema();
    List<Struct> envelopes =
        emitEnvelopes(
            config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
            schema,
            List.of(
                valuedRow(OperationType.PRE_IMAGE, 0, false, schema, 0, Map.of("t", "old-t")),
                valuedRow(OperationType.ROW_UPDATE, 1, false, schema, null, Map.of("s", "s1")),
                valuedRow(OperationType.ROW_UPDATE, 2, false, schema, 0, Map.of("t", "t1")),
                valuedRow(OperationType.POST_IMAGE, 3, false, schema, null, Map.of("s", "s1")),
                valuedRow(
                    OperationType.POST_IMAGE, 4, true, schema, 0, Map.of("s", "s1", "t", "t1"))));
    assertEquals(2, envelopes.size());
    Struct clusteredRecord = null;
    Struct staticRecord = null;
    for (Struct envelope : envelopes) {
      if (envelope.getStruct(Envelope.FieldName.AFTER).get("t") != null) {
        clusteredRecord = envelope;
      } else {
        staticRecord = envelope;
      }
    }
    assertNotNull(clusteredRecord);
    assertNotNull(staticRecord);
    assertEquals("old-t", clusteredRecord.getStruct(Envelope.FieldName.BEFORE).get("t"));
    assertEquals("t1", clusteredRecord.getStruct(Envelope.FieldName.AFTER).get("t"));
    assertNull(staticRecord.get(Envelope.FieldName.BEFORE));
    assertEquals("s1", staticRecord.getStruct(Envelope.FieldName.AFTER).get("s"));
  }

  /**
   * Deleting a missing partition writes no PRE_IMAGE (#368). Both deletes must be dispatched, with
   * or without their preimages.
   */
  private void assertPartitionDeletesDispatched(ScyllaConnectorConfig config)
      throws InterruptedException {
    ChangeSchema schema = pkOnlySchema();
    runScenario(
        config,
        Optional.of(changeId(10_500)),
        // A version with partition-delete preimages, where the removed wait used to apply
        c ->
            new ScyllaChangesConsumer(
                dispatcher,
                offsetContext,
                null,
                Clock.SYSTEM,
                c,
                ScyllaVersion.PARTITION_DELETE_PREIMAGE_SUPPORT,
                Long.MAX_VALUE),
        List.of(
            row(OperationType.PARTITION_DELETE, 11_000, true, 0, null, schema),
            row(OperationType.PRE_IMAGE, 12_000, null, 0, null, schema),
            row(OperationType.PARTITION_DELETE, 12_000, true, 1, null, schema)));
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
            row(OperationType.ROW_UPDATE, 12_000, null, 0, null, schema),
            row(OperationType.ROW_UPDATE, 12_000, true, 1, 0, schema));
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE), Optional.empty(), this::advanced, rows);
    assertFalse(snapshots.get(0).offset.containsKey(SourceInfo.CHANGE_ID_TIME));
    assertEquals(
        rows.get(1).getId().getChangeTime().getUUID().toString(),
        snapshots.get(1).offset.get(SourceInfo.CHANGE_ID_TIME));
  }

  /**
   * Per-row TTL expiry (ScyllaDB 2026.2+, #366) as the server logs it: a batch of its own, the
   * preimage then the service delete.
   */
  private static List<RawChange> serviceDeleteBatch(OperationType op, ChangeSchema schema) {
    Integer ck = op == OperationType.SERVICE_ROW_DELETE ? 0 : null;
    return List.of(
        row(OperationType.PRE_IMAGE, 11_000, null, 0, ck, schema),
        row(op, 11_000, true, 1, ck, schema));
  }

  private void assertServiceDeleteIsDispatchedAsDelete(
      Function<ScyllaConnectorConfig, TaskAndRawChangeConsumer> consumerFactory)
      throws InterruptedException {
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        consumerFactory,
        serviceDeleteBatch(OperationType.SERVICE_ROW_DELETE, schema()));
    assertEquals(List.of(Envelope.Operation.DELETE), operations);
    clearDispatched();
    runScenario(
        config(CdcIncludeMode.NONE, CdcIncludeMode.NONE),
        Optional.of(changeId(10_500)),
        consumerFactory,
        serviceDeleteBatch(OperationType.SERVICE_PARTITION_DELETE, pkOnlySchema()));
    assertEquals(List.of(Envelope.Operation.DELETE), operations);
  }

  @Test
  void serviceDelete_isDispatchedAsDelete() throws InterruptedException {
    assertServiceDeleteIsDispatchedAsDelete(this::advanced);
  }

  @Test
  void legacy_serviceDelete_isDispatchedAsDelete() throws InterruptedException {
    assertServiceDeleteIsDispatchedAsDelete(this::legacy);
  }

  /**
   * Like a client partition delete, a service one is dropped when the table has a clustering key.
   */
  private void assertServicePartitionDeleteWithClusteringKeyIsIgnored(
      Function<ScyllaConnectorConfig, TaskAndRawChangeConsumer> consumerFactory)
      throws InterruptedException {
    ScyllaConnectorConfig config = config(CdcIncludeMode.NONE, CdcIncludeMode.NONE);
    Optional<ChangeId> lastConsumed = Optional.of(changeId(10_500));
    setUp(config, lastConsumed);
    feed(
        consumerFactory.apply(config),
        lastConsumed,
        serviceDeleteBatch(OperationType.SERVICE_PARTITION_DELETE, schema()));
    assertTrue(snapshots.isEmpty(), "dispatched " + operations);
  }

  @Test
  void servicePartitionDelete_withClusteringKey_isIgnored() throws InterruptedException {
    assertServicePartitionDeleteWithClusteringKeyIsIgnored(this::advanced);
  }

  @Test
  void legacy_servicePartitionDelete_withClusteringKey_isIgnored() throws InterruptedException {
    assertServicePartitionDeleteWithClusteringKeyIsIgnored(this::legacy);
  }
}
