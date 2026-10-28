package com.scylladb.cdc.debezium.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
  private final List<Envelope.Operation> operations = new ArrayList<>();
  private int currentRow;

  /** Thrown by the next dispatch, if set. */
  private RuntimeException nextDispatchFailure;

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
              if (nextDispatchFailure != null) {
                RuntimeException failure = nextDispatchFailure;
                nextDispatchFailure = null;
                throw failure;
              }
              Object emitter = invocation.getArgument(2);
              if (emitter instanceof ScyllaChangeRecordEmitter) {
                operations.add(((ScyllaChangeRecordEmitter) emitter).getOperation());
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
    for (CdcIncludeMode before : List.of(CdcIncludeMode.NONE, CdcIncludeMode.FULL)) {
      for (CdcIncludeMode after : List.of(CdcIncludeMode.NONE, CdcIncludeMode.FULL)) {
        snapshots.clear();
        operations.clear();
        assertUpsertThenDelete(before, after, OperationType.ROW_UPDATE, Envelope.Operation.UPDATE);
      }
    }
  }

  @Test
  void insertThenDelete_emitsBothRecordsInEveryMode() throws InterruptedException {
    for (CdcIncludeMode before : List.of(CdcIncludeMode.NONE, CdcIncludeMode.FULL)) {
      for (CdcIncludeMode after : List.of(CdcIncludeMode.NONE, CdcIncludeMode.FULL)) {
        snapshots.clear();
        operations.clear();
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
    ChangeSchema.ColumnDefinition set = Mockito.mock(ChangeSchema.ColumnDefinition.class);
    Mockito.when(set.getColumnName()).thenReturn("s");
    Mockito.when(set.getBaseTableColumnKind()).thenReturn(ChangeSchema.ColumnKind.REGULAR);
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
   * {@code INSERT ... USING TTL} with a non-frozen collection: ScyllaDB logs the INSERT, the
   * collection tombstone and the collection cells as separate deltas of one batch (#365).
   */
  private static List<RawChange> ttlInsertWithCollectionBatch() {
    ChangeSchema schema = schema();
    return List.of(
        row(OperationType.ROW_INSERT, 12_000, null, 0, 0, schema),
        row(OperationType.ROW_UPDATE, 12_000, null, 1, 0, schema),
        row(OperationType.ROW_UPDATE, 12_000, null, 2, 0, schema),
        row(OperationType.POST_IMAGE, 12_000, true, 3, 0, schema));
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
  void updateWithoutPreimage_bothImages_isDispatchedAtEndOfBatch() throws InterruptedException {
    ChangeSchema schema = schema();
    runScenario(
        config(CdcIncludeMode.FULL, CdcIncludeMode.FULL),
        Optional.of(changeId(10_500)),
        this::advanced,
        List.of(
            row(OperationType.ROW_UPDATE, 12_000, null, 0, 0, schema),
            row(OperationType.POST_IMAGE, 12_000, true, 1, 0, schema)));
    assertEquals(List.of(Envelope.Operation.UPDATE), operations);
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
}
