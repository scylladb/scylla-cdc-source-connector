package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.debezium.connector.ScyllaConnectorConfig.CdcIncludeMode;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.ChangeId;
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
   * Default timeout for incomplete tasks in milliseconds. Tasks that remain incomplete longer than
   * this duration will be cleaned up to prevent memory leaks.
   */
  private static final long DEFAULT_INCOMPLETE_TASK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

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
  private final CdcIncludeMode cdcIncludeBefore;
  private final CdcIncludeMode cdcIncludeAfter;
  private final Map<TaskId, TaskInfo> taskInfoMap;
  private final ScyllaConnectorConfig connectorConfig;
  private final long incompleteTaskTimeoutMs;
  private final AtomicLong eventCounter = new AtomicLong(0);

  public ScyllaChangesConsumer(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    this(
        dispatcher,
        offsetContext,
        schema,
        clock,
        connectorConfig,
        DEFAULT_INCOMPLETE_TASK_TIMEOUT_MS);
  }

  /**
   * Constructor with configurable timeout for testing.
   *
   * @param dispatcher the event dispatcher
   * @param offsetContext the offset context
   * @param schema the schema
   * @param clock the clock
   * @param connectorConfig the connector configuration
   * @param incompleteTaskTimeoutMs timeout for incomplete tasks in milliseconds
   */
  ScyllaChangesConsumer(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchema schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig,
      long incompleteTaskTimeoutMs) {
    this.dispatcher = dispatcher;
    this.offsetContext = offsetContext;
    this.schema = schema;
    this.clock = clock;
    this.connectorConfig = connectorConfig;
    this.incompleteTaskTimeoutMs = incompleteTaskTimeoutMs;
    this.cdcIncludeBefore = connectorConfig.getCdcIncludeBefore();
    this.cdcIncludeAfter = connectorConfig.getCdcIncludeAfter();

    this.usePreimages = cdcIncludeBefore.requiresImage();
    // Use postimages if the new config requires image data
    this.usePostimages = cdcIncludeAfter.requiresImage();

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
   * Gets or creates a TaskInfo for the given task ID.
   *
   * @param taskId the task ID
   * @return the TaskInfo, never null when taskInfoMap is initialized
   * @throws IllegalStateException if called when taskInfoMap is null (preimages/postimages
   *     disabled)
   */
  private TaskInfo getOrCreateTaskInfo(TaskId taskId) {
    if (taskInfoMap == null) {
      throw new IllegalStateException(
          "getOrCreateTaskInfo called but taskInfoMap is null. "
              + "This method should only be called when preimages or postimages are enabled.");
    }
    return taskInfoMap.computeIfAbsent(taskId, k -> createTaskInfo());
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
    Iterator<Map.Entry<TaskId, TaskInfo>> iterator = taskInfoMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<TaskId, TaskInfo> entry = iterator.next();
      TaskInfo taskInfo = entry.getValue();
      long age = now - taskInfo.getCreatedAtMillis();

      if (age > incompleteTaskTimeoutMs) {
        logger.warn(
            "Removing stale incomplete task {} after {}ms. "
                + "This may indicate missing preimage/postimage events. "
                + "Task state: change={}, preImage={}, postImage={}",
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
        TaskInfo taskInfo = null;
        switch (operationType) {
          case PRE_IMAGE:
            taskInfo = getOrCreateTaskInfo(task.id).setPreImage(change);
            break;
          case POST_IMAGE:
            taskInfo = getOrCreateTaskInfo(task.id).setPostImage(change);
            break;
          case ROW_DELETE:
          case ROW_INSERT:
          case ROW_UPDATE:
            taskInfo = getOrCreateTaskInfo(task.id).setChange(change);
            break;
          case PARTITION_DELETE:
            if (isSinglePartitionDelete(change.getSchema())) {
              taskInfo = getOrCreateTaskInfo(task.id).setChange(change);
            }
            break;
          default:
        }
        if (taskInfo == null) {
          // If it is event that consumer ignores we advance the state without dispatching
          advanceStateWithoutDispatching(task, change.getId());
        } else if (taskInfo.isComplete()) {
          // If it is final event that task expects we advance the state and dispatch
          taskInfoMap.remove(task.id);
          advanceStateAndDispatch(task, change.getId(), taskInfo);
        }
        // If task is not complete yet (e.g., received PRE_IMAGE but waiting for change),
        // do NOT advance state - just store the event and return. The state will be
        // advanced when the task becomes complete.
        return CompletableFuture.completedFuture(null);
      }
      switch (operationType) {
        case ROW_DELETE:
        case ROW_INSERT:
        case ROW_UPDATE:
          advanceStateAndDispatch(task, change.getId(), new TaskInfo.Basic().setChange(change));
          break;
        case PARTITION_DELETE:
          if (isSinglePartitionDelete(change.getSchema())) {
            advanceStateAndDispatch(task, change.getId(), new TaskInfo.Basic().setChange(change));
          } else {
            advanceStateWithoutDispatching(task, change.getId());
          }
          break;
        default:
          advanceStateWithoutDispatching(task, change.getId());
      }
    } catch (InterruptedException e) {
      logger.error(
          "InterruptedException in consume for change {}: {}",
          change.getId().toString(),
          e.getMessage(),
          e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      logger.error(
          "Exception in consume for change {}: {}", change.getId().toString(), e.getMessage(), e);
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

  TaskStateOffsetContext advanceStateWithoutDispatching(Task task, ChangeId changeId) {
    Task updatedTask = task.updateState(changeId);
    TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task.id);
    taskStateOffsetContext.dataChangeEvent(updatedTask.state);
    return taskStateOffsetContext;
  }

  void advanceStateAndDispatch(Task task, ChangeId changeId, TaskInfo taskInfo)
      throws InterruptedException {
    TaskStateOffsetContext taskStateOffsetContext = advanceStateWithoutDispatching(task, changeId);
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
