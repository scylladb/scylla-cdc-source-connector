package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.Task;
import com.scylladb.cdc.model.worker.TaskAndRawChangeConsumer;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.util.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy consumer that uses simple HashMap-based preimage storage.
 *
 * <p>This is the v1 consumer that uses {@code experimental.preimages.enabled} for preimage support.
 * It does not support postimages.
 *
 * <p>Used when {@code cdc.output.format=legacy} is configured.
 */
public class ScyllaChangesConsumerLegacy implements TaskAndRawChangeConsumer {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Threshold for logging warnings about preimage map size. */
  private static final int PREIMAGE_MAP_SIZE_WARNING_THRESHOLD = 1000;

  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final ScyllaOffsetContext offsetContext;
  private final ScyllaSchemaLegacy schema;
  private final Clock clock;
  private final boolean usePreimages;
  private final Map<TaskId, RawChange> lastPreImage;
  private final ScyllaConnectorConfig connectorConfig;

  public ScyllaChangesConsumerLegacy(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchemaLegacy schema,
      Clock clock,
      ScyllaConnectorConfig connectorConfig) {
    this.dispatcher = dispatcher;
    this.offsetContext = offsetContext;
    this.schema = schema;
    this.clock = clock;
    this.connectorConfig = connectorConfig;
    this.usePreimages = connectorConfig.getPreimagesEnabled();
    if (usePreimages) {
      this.lastPreImage = new HashMap<>();
    } else {
      this.lastPreImage = null;
    }
  }

  @Override
  public CompletableFuture<Void> consume(Task task, RawChange change) {
    try {
      if (usePreimages && change.getOperationType() == RawChange.OperationType.PRE_IMAGE) {
        lastPreImage.put(task.id, change);
        if (lastPreImage.size() == PREIMAGE_MAP_SIZE_WARNING_THRESHOLD) {
          logger.warn(
              "Preimage map has grown to {} entries. This may indicate orphaned preimages "
                  + "that are not being consumed by corresponding change records.",
              lastPreImage.size());
        }
        return CompletableFuture.completedFuture(null);
      }

      Task updatedTask = task.updateState(change.getId());
      TaskStateOffsetContext taskStateOffsetContext = offsetContext.taskStateOffsetContext(task.id);
      taskStateOffsetContext.dataChangeEvent(updatedTask.state);

      RawChange.OperationType operationType = change.getOperationType();
      ChangeSchema changeSchema = change.getSchema();
      if (operationType == RawChange.OperationType.PARTITION_DELETE) {
        // The connector currently does not support partition deletes.
        //
        // However, there is a single exception to this:
        // If the (base) table's primary key consists only of
        // partition key, row DELETEs in such a table are represented as
        // partition deletes (even though they affect at most a single row).
        // In that case, we will interpret such a CDC operation
        // as a "standard" ROW_DELETE.
        boolean hasClusteringColumn =
            changeSchema.getNonCdcColumnDefinitions().stream()
                .anyMatch(
                    column ->
                        column.getBaseTableColumnType() == ChangeSchema.ColumnType.CLUSTERING_KEY);
        if (hasClusteringColumn) {
          return CompletableFuture.completedFuture(null);
        }
      } else if (operationType != RawChange.OperationType.ROW_INSERT
          && operationType != RawChange.OperationType.ROW_UPDATE
          && operationType != RawChange.OperationType.ROW_DELETE) {
        return CompletableFuture.completedFuture(null);
      }

      if (usePreimages && lastPreImage.containsKey(task.id)) {
        dispatcher.dispatchDataChangeEvent(
            new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
            new CollectionId(task.id.getTable()),
            new ScyllaChangeRecordEmitterLegacy(
                new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
                lastPreImage.get(task.id),
                change,
                taskStateOffsetContext,
                schema,
                clock,
                connectorConfig));
        lastPreImage.remove(task.id);
      } else {
        dispatcher.dispatchDataChangeEvent(
            new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
            new CollectionId(task.id.getTable()),
            new ScyllaChangeRecordEmitterLegacy(
                new ScyllaPartition(offsetContext, taskStateOffsetContext.sourceInfo),
                null,
                change,
                taskStateOffsetContext,
                schema,
                clock,
                connectorConfig));
      }
    } catch (InterruptedException e) {
      logger.error("Exception while dispatching change: {}", change.getId().toString());
      logger.error("Exception details: {}", e.getMessage());
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.error(
          "Unexpected exception while dispatching change: {}", change.getId().toString(), e);
      throw new RuntimeException("Failed to dispatch CDC change event", e);
    }
    return CompletableFuture.completedFuture(null);
  }

  /** Returns the preimage map for testing purposes. Package-private for test access. */
  Map<TaskId, RawChange> getPreImageMapForTesting() {
    return lastPreImage;
  }
}
