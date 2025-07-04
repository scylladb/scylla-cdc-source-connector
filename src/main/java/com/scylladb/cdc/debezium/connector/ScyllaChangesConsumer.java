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

public class ScyllaChangesConsumer implements TaskAndRawChangeConsumer {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final EventDispatcher<ScyllaPartition, CollectionId> dispatcher;
  private final ScyllaOffsetContext offsetContext;
  private final ScyllaSchema schema;
  private final Clock clock;
  private final boolean usePreimages;
  private final Map<TaskId, RawChange> lastPreImage;
  private final ScyllaConnectorConfig connectorConfig;

  public ScyllaChangesConsumer(
      EventDispatcher<ScyllaPartition, CollectionId> dispatcher,
      ScyllaOffsetContext offsetContext,
      ScyllaSchema schema,
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
      logger.trace("Consuming RawChange of type {}", change.getOperationType());
      if (usePreimages && change.getOperationType() == RawChange.OperationType.PRE_IMAGE) {
        lastPreImage.put(task.id, change);
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
            new ScyllaChangeRecordEmitter(
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
            new ScyllaChangeRecordEmitter(
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
    }
    return CompletableFuture.completedFuture(null);
  }
}
