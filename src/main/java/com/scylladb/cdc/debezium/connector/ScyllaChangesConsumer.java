package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes CDC changes from Scylla and dispatches them to Kafka.
 *
 * <p>Rows are buffered until the end of their CDC batch, then dispatched as records: per base-table
 * row, its upsert deltas merged into one record (see {@link MergedRawChange}), followed by a record
 * for its delete if the batch also deletes it. Each record carries its preimage/postimage when
 * configured. ScyllaDB marks the batch's last row, so this does not depend on which images a given
 * operation has (a new row has no preimage, a delete no postimage).
 */
public class ScyllaChangesConsumer
    implements TaskAndRawChangeConsumer, ScyllaWorkerTransport.WindowEndListener {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Error marker prefix used in log messages for connector errors during message handling. This
   * marker can be searched for in logs to identify connector-level errors.
   */
  public static final String CONNECTOR_ERROR_MARKER = "[SCYLLA-CDC-CONNECTOR-ERROR]";

  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final ScyllaOffsetContext offsetContext;
  private final ScyllaSchema schema;
  private final Clock clock;
  private final boolean usePreimages;
  private final boolean usePostimages;
  // The batch each task is reading. A task's rows arrive in order on one consume() at a time, so a
  // batch is only touched by its own task.
  private final Map<TaskId, PendingBatch> pendingBatches = new ConcurrentHashMap<>();
  private final ScyllaConnectorConfig connectorConfig;
  private final long incompleteTaskTimeoutMs;

  /** The rows of one task's current CDC batch, buffered until the batch's last row. */
  private static final class PendingBatch {
    // The task as of the batch's first row; it only advances on an end-of-batch row.
    private final Task task;
    private final ChangeId changeId;
    private final long createdAtMillis = System.currentTimeMillis();
    private int lastBatchSeqNo = -1;
    private boolean warnedSlow;
    // The record each row is currently building.
    private final Map<RowKey, TaskInfo> openRecords = new HashMap<>();
    // All records of the batch, in the order they were opened.
    private final List<TaskInfo> records = new ArrayList<>();

    private PendingBatch(Task task, ChangeId changeId) {
      this.task = task;
      this.changeId = changeId;
    }

    private TaskInfo open(RowKey rowKey, TaskInfo taskInfo) {
      openRecords.put(rowKey, taskInfo);
      records.add(taskInfo);
      return taskInfo;
    }
  }

  public ScyllaChangesConsumer(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig,
      ScyllaVersion scyllaVersion) {
    this(
        dispatcher,
        offsetContext,
        schema,
        clock,
        connectorConfig,
        scyllaVersion,
        connectorConfig.getIncompleteTaskTimeoutMs());
  }

  /**
   * Constructor with configurable timeout for testing.
   *
   * @param dispatcher the event dispatcher
   * @param offsetContext the offset context
   * @param schema the schema
   * @param clock the clock
   * @param connectorConfig the connector configuration
   * @param scyllaVersion the detected Scylla version, may be null; not used yet, #366 will need it
   * @param incompleteTaskTimeoutMs how long after a batch's first row a further row logs a warning
   */
  ScyllaChangesConsumer(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig,
      ScyllaVersion scyllaVersion,
      long incompleteTaskTimeoutMs) {
    this.dispatcher = dispatcher;
    this.offsetContext = offsetContext;
    this.schema = schema;
    this.clock = clock;
    this.connectorConfig = connectorConfig;
    this.incompleteTaskTimeoutMs = incompleteTaskTimeoutMs;

    this.usePreimages = connectorConfig.getCdcIncludeBefore().requiresImage();
    // Use postimages if the new config requires image data
    this.usePostimages = connectorConfig.getCdcIncludeAfter().requiresImage();
  }

  private TaskInfo createTaskInfo() {
    if (usePreimages && usePostimages) {
      return new TaskInfo.BeforeAfter();
    } else if (usePreimages) {
      return new TaskInfo.Before();
    } else if (usePostimages) {
      return new TaskInfo.After();
    } else {
      return new TaskInfo.Basic();
    }
  }

  private PendingBatch pendingBatch(Task task, RawChange change) {
    return pendingBatches.computeIfAbsent(task.id, id -> new PendingBatch(task, change.getId()));
  }

  /** The record the row is building in the current batch, opening one if it has none. */
  private TaskInfo openRecord(Task task, RawChange change, RowKey rowKey) {
    PendingBatch batch = pendingBatch(task, change);
    TaskInfo taskInfo = batch.openRecords.get(rowKey);
    return taskInfo != null ? taskInfo : batch.open(rowKey, createTaskInfo());
  }

  private static boolean isDelete(RawChange change) {
    RawChange.OperationType operationType = change.getOperationType();
    return operationType == RawChange.OperationType.ROW_DELETE
        || operationType == RawChange.OperationType.PARTITION_DELETE;
  }

  /**
   * The record a delta joins: upserts merge into one record. A delete, or an upsert setting a
   * column the record already sets (see {@link MergedRawChange#overlaps}), ends it and gets a
   * record of its own, dispatched after it. ScyllaDB logs a row's upserts before its deletion. The
   * new record shares the batch's preimage, the row state before the batch.
   */
  private TaskInfo recordForDelta(Task task, RawChange change, RowKey rowKey) {
    TaskInfo taskInfo = openRecord(task, change, rowKey);
    RawChange current = taskInfo.getChange();
    if (current == null
        || (isDelete(current) == isDelete(change) && !MergedRawChange.overlaps(current, change))) {
      return taskInfo;
    }
    return pendingBatch(task, change)
        .open(rowKey, createTaskInfo().setPreImage(taskInfo.getPreImage()));
  }

  /**
   * Handles a row arriving while the task has a batch pending. scylla-cdc-java reads a stream's
   * rows in (cdc$time, batch_seq_no) order and a batch shares one cdc$time, so a batch never spans
   * two windows or interleaves with another one. After a failure the library re-reads from its last
   * end-of-batch row, so a row of the same batch with a sequence number already seen, or a row of
   * an earlier batch, means a re-read: the buffered rows are dropped and rebuilt. A row of a later
   * batch means the pending batch has no more rows in the log.
   */
  private void beforeRow(Task task, RawChange change) throws InterruptedException {
    PendingBatch batch = pendingBatches.get(task.id);
    if (batch == null) {
      return;
    }
    int order = change.getId().compareTo(batch.changeId);
    if (order > 0) {
      logger.warn(
          "CDC batch {} has no end-of-batch row before the next batch; dispatching it",
          batch.changeId);
      dispatchPendingBatch(batch, null);
    } else if (order < 0 || change.getBatchSequenceNumber() <= batch.lastBatchSeqNo) {
      logger.info("Re-reading CDC batch {}; dropping its buffered rows", batch.changeId);
      pendingBatches.remove(task.id);
    } else if (!batch.warnedSlow
        && System.currentTimeMillis() - batch.createdAtMillis > incompleteTaskTimeoutMs) {
      batch.warnedSlow = true;
      logger.warn(
          "CDC batch {} is still being read after {}ms; its records wait for its last row",
          batch.changeId,
          incompleteTaskTimeoutMs);
    }
  }

  /**
   * Dispatches the task's pending batch before its offset moves to the next window, so the window
   * checkpoint never passes undispatched rows. The window's rows are all read by then, so the batch
   * has no more rows.
   */
  @Override
  public void windowEnded(TaskId taskId) throws InterruptedException {
    PendingBatch batch = pendingBatches.get(taskId);
    if (batch != null) {
      logger.warn(
          "CDC batch {} has no end-of-batch row in its window; dispatching it", batch.changeId);
      dispatchPendingBatch(batch, null);
    }
  }

  @Override
  public CompletableFuture<Void> consume(Task task, RawChange change) {
    try {
      logger.debug("Consuming RawChange of type {}", change.getOperationType());
      beforeRow(task, change);

      RawChange.OperationType operationType = change.getOperationType();
      RowKey rowKey = RowKey.from(task.id, change);
      boolean buffered = true;
      switch (operationType) {
        case PRE_IMAGE:
          if (usePreimages) {
            openRecord(task, change, rowKey).setPreImage(change);
          } else {
            buffered = false;
          }
          break;
        case POST_IMAGE:
          if (usePostimages) {
            openRecord(task, change, rowKey).setPostImage(change);
          } else {
            buffered = false;
          }
          break;
        case ROW_DELETE:
        case ROW_INSERT:
        case ROW_UPDATE:
          recordForDelta(task, change, rowKey).setChange(change);
          break;
        case PARTITION_DELETE:
          if (isSinglePartitionDelete(change.getSchema())) {
            recordForDelta(task, change, rowKey).setChange(change);
          } else {
            buffered = false;
          }
          break;
        default:
          buffered = false;
      }
      PendingBatch batch = pendingBatches.get(task.id);
      if (batch != null) {
        batch.lastBatchSeqNo = change.getBatchSequenceNumber();
      }
      if (change.isEndOfBatch()) {
        if (batch != null) {
          dispatchPendingBatch(batch, change);
        } else {
          updateOffset(task, change);
        }
      } else if (!buffered) {
        // If it is event that consumer ignores we record the offset without dispatching
        updateOffset(task, change);
      }
      // A buffered row mid-batch records no offset: that happens when its batch is dispatched.
    } catch (InterruptedException e) {
      logger.error(
          "{} InterruptedException in consume for change {}: {}",
          CONNECTOR_ERROR_MARKER,
          change.getId().toString(),
          e.getMessage(),
          e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      logger.error(
          "{} Exception in consume for change {}: {}",
          CONNECTOR_ERROR_MARKER,
          change.getId().toString(),
          e.getMessage(),
          e);
      throw e;
    }
    return CompletableFuture.completedFuture(null);
  }

  static boolean isSinglePartitionDelete(ChangeSchema changeSchema) {
    // The connector currently does not support partition deletes.
    //
    // However, there is a single exception to this:
    // If the (base) table's primary key consists only of
    // partition key, row DELETEs in such a table are represented as
    // partition deletes (even though they affect at most a single row).
    // In that case, we will interpret such a CDC operation
    // as a "standard" ROW_DELETE.
    return !changeSchema.getNonCdcColumnDefinitions().stream()
        .anyMatch(
            column -> column.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY);
  }

  /**
   * Dispatches every record of {@code batch} and forgets it. Only the last record carries the
   * batch's own offset (#340): until then a restart must re-read the batch. The batch stays pending
   * until every record is out: a failed window move is retried without re-reading the window.
   *
   * @param endOfBatch the batch's last row, or null if the log has no more rows for the batch
   */
  private void dispatchPendingBatch(PendingBatch batch, RawChange endOfBatch)
      throws InterruptedException {
    List<TaskInfo> ready = new ArrayList<>();
    for (TaskInfo taskInfo : batch.records) {
      if (taskInfo.getChange() == null) {
        // Images whose delta the connector does not emit, e.g. the preimage of a per-row TTL
        // service delete (#366).
        logger.debug("Skipping CDC images without a supported change in {}", batch.changeId);
        continue;
      }
      ready.add(taskInfo);
    }
    TaskStateOffsetContext taskStateOffsetContext =
        offsetContext.taskStateOffsetContext(batch.task.id);
    if (ready.isEmpty()) {
      taskStateOffsetContext.dataChangeEvent(batch.task, batch.changeId, true);
      pendingBatches.remove(batch.task.id);
      return;
    }
    for (int i = 0; i < ready.size(); i++) {
      TaskInfo taskInfo = ready.get(i);
      boolean last = i == ready.size() - 1;
      ChangeId changeId =
          last && endOfBatch != null ? endOfBatch.getId() : taskInfo.getChange().getId();
      taskStateOffsetContext.dataChangeEvent(batch.task, changeId, last);
      dispatch(batch.task, taskStateOffsetContext, taskInfo);
    }
    pendingBatches.remove(batch.task.id);
  }

  private void updateOffset(Task task, RawChange change) {
    offsetContext.taskStateOffsetContext(task.id).dataChangeEvent(task, change);
  }

  private void dispatch(Task task, TaskStateOffsetContext taskStateOffsetContext, TaskInfo taskInfo)
      throws InterruptedException {
    dispatcher.dispatchDataChangeEvent(
        new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
        new CollectionId(task.id.getTable()),
        new ScyllaChangeRecordEmitter(
            new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
            taskInfo,
            taskStateOffsetContext,
            schema,
            clock,
            connectorConfig));
  }
}
