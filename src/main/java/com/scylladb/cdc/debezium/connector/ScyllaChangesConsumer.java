package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumes CDC changes from Scylla and dispatches them to Kafka.
 *
 * <p>This consumer handles the accumulation of preimage/postimage events when configured, ensuring
 * all required events are collected before dispatching a complete change record.
 */
public class ScyllaChangesConsumer implements TaskAndRawChangeConsumer {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Error marker prefix used in log messages for connector errors during message handling. This
   * marker can be searched for in logs to identify connector-level errors.
   */
  public static final String CONNECTOR_ERROR_MARKER = "[SCYLLA-CDC-CONNECTOR-ERROR]";

  /**
   * Interval between cleanup checks in terms of number of events processed. Cleanup runs
   * approximately every N events to avoid checking on every single event.
   */
  private static final long CLEANUP_CHECK_INTERVAL = 1000;

  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final ScyllaOffsetContext offsetContext;
  private final ScyllaSchema schema;
  private final Clock clock;
  private final boolean usePreimages;
  private final boolean usePostimages;
  private final Map<RowKey, TaskInfo> taskInfoMap;
  private final ScyllaConnectorConfig connectorConfig;
  private final long incompleteTaskTimeoutMs;
  private final AtomicLong eventCounter = new AtomicLong(0);

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
   * @param scyllaVersion the detected Scylla version, may be null; not used yet
   * @param incompleteTaskTimeoutMs timeout for incomplete tasks in milliseconds
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

    if (usePreimages || usePostimages) {
      // Use ConcurrentHashMap for thread safety
      this.taskInfoMap = new ConcurrentHashMap<>();
    } else {
      this.taskInfoMap = null;
    }
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

  /**
   * Gets or creates a TaskInfo for the given row key.
   *
   * @param rowKey the row key identifying the specific row within the task
   * @return the TaskInfo, never null when taskInfoMap is initialized
   * @throws IllegalStateException if called when taskInfoMap is null (preimages/postimages
   *     disabled)
   */
  private TaskInfo getOrCreateTaskInfo(RowKey rowKey) {
    if (taskInfoMap == null) {
      throw new IllegalStateException(
          "getOrCreateTaskInfo called but taskInfoMap is null. "
              + "This method should only be called when preimages or postimages are enabled.");
    }
    return taskInfoMap.computeIfAbsent(rowKey, k -> createTaskInfo());
  }

  /**
   * Cleans up stale incomplete tasks to prevent memory leaks.
   *
   * <p>Tasks that have been waiting for completion longer than the configured timeout are removed
   * and logged as warnings.
   */
  private void cleanupStaleTasks() {
    if (taskInfoMap == null || taskInfoMap.isEmpty()) {
      return;
    }

    long now = System.currentTimeMillis();
    Iterator<Map.Entry<RowKey, TaskInfo>> iterator = taskInfoMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<RowKey, TaskInfo> entry = iterator.next();
      TaskInfo taskInfo = entry.getValue();
      long age = now - taskInfo.getCreatedAtMillis();

      if (age > incompleteTaskTimeoutMs) {
        logger.error(
            "{} Dropping stale incomplete task {} after {}ms. "
                + "This may indicate missing preimage/postimage events. "
                + "Task state: change={}, preImage={}, postImage={}",
            CONNECTOR_ERROR_MARKER,
            entry.getKey(),
            age,
            taskInfo.getChange() != null,
            taskInfo.getPreImage() != null,
            taskInfo.getPostImage() != null);
        iterator.remove();
      }
    }
  }

  /**
   * Periodically triggers cleanup based on event count.
   *
   * <p>This avoids checking on every event while still ensuring timely cleanup.
   */
  private void maybeCleanupStaleTasks() {
    if (eventCounter.incrementAndGet() % CLEANUP_CHECK_INTERVAL == 0) {
      cleanupStaleTasks();
    }
  }

  @Override
  public CompletableFuture<Void> consume(Task task, RawChange change) {
    try {
      logger.debug("Consuming RawChange of type {}", change.getOperationType());

      // Periodically clean up stale incomplete tasks to prevent memory leaks
      maybeCleanupStaleTasks();

      RawChange.OperationType operationType = change.getOperationType();

      if (taskInfoMap != null) {
        RowKey rowKey = RowKey.from(task.id, change);
        TaskInfo taskInfo = null;
        switch (operationType) {
          case PRE_IMAGE:
            taskInfo = getOrCreateTaskInfo(rowKey).setPreImage(change);
            break;
          case POST_IMAGE:
            taskInfo = getOrCreateTaskInfo(rowKey).setPostImage(change);
            break;
          case ROW_DELETE:
          case ROW_INSERT:
          case ROW_UPDATE:
            taskInfo = getOrCreateTaskInfo(rowKey).setChange(change);
            break;
          case PARTITION_DELETE:
            if (isSinglePartitionDelete(change.getSchema())) {
              // Completes on arrival, with the preimage if one was already stored
              taskInfo = getOrCreateTaskInfo(rowKey).setChange(change);
            }
            break;
          default:
        }
        if (taskInfo == null) {
          // If it is event that consumer ignores we record the offset without dispatching
          updateOffset(task, change);
        } else if (taskInfo.isComplete()) {
          // If it is final event that task expects we record the offset and dispatch
          taskInfoMap.remove(rowKey);
          updateOffsetAndDispatch(task, change, taskInfo);
        }
        // If task is not complete yet (e.g., received PRE_IMAGE but waiting for change),
        // just store the event and return. The offset is recorded when the task completes.
        return CompletableFuture.completedFuture(null);
      }
      switch (operationType) {
        case ROW_DELETE:
        case ROW_INSERT:
        case ROW_UPDATE:
          updateOffsetAndDispatch(task, change, new TaskInfo.Basic().setChange(change));
          break;
        case PARTITION_DELETE:
          if (isSinglePartitionDelete(change.getSchema())) {
            updateOffsetAndDispatch(task, change, new TaskInfo.Basic().setChange(change));
          } else {
            updateOffset(task, change);
          }
          break;
        default:
          updateOffset(task, change);
      }
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

  TaskStateOffsetContext updateOffset(Task task, RawChange change) {
    TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task.id);
    taskStateOffsetContext.dataChangeEvent(task, change);
    return taskStateOffsetContext;
  }

  void updateOffsetAndDispatch(Task task, RawChange change, TaskInfo taskInfo)
      throws InterruptedException {
    TaskStateOffsetContext taskStateOffsetContext = updateOffset(task, change);
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
