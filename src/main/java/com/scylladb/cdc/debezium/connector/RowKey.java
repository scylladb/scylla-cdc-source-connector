package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Composite key identifying a specific row within a CDC task's scope.
 *
 * <p>This key combines a {@link TaskId} (which identifies a vnode + table) with the row's primary
 * key values (partition key + clustering key columns). This ensures that when multiple rows in the
 * same partition are modified in a single batch, each row gets its own accumulator for
 * preimage/postimage correlation.
 *
 * <p>Without this, all rows sharing the same partition (and therefore the same {@code TaskId})
 * would collide in the consumer's storage maps, causing preimage misalignment or event loss when
 * CDC log entries arrive in type-grouped order.
 *
 * @see ScyllaChangesConsumer
 * @see ScyllaChangesConsumerLegacy
 */
public final class RowKey {

  private final TaskId taskId;
  private final List<Object> primaryKeyValues;

  private RowKey(TaskId taskId, List<Object> primaryKeyValues) {
    this.taskId = Objects.requireNonNull(taskId, "taskId must not be null");
    this.primaryKeyValues =
        Collections.unmodifiableList(
            Objects.requireNonNull(primaryKeyValues, "primaryKeyValues must not be null"));
  }

  /**
   * Creates a {@code RowKey} from a task ID and a CDC change event.
   *
   * <p>Extracts all partition key and clustering key column values from the change to form a unique
   * row identifier within the task's scope.
   *
   * @param taskId the task ID (identifies vnode + table)
   * @param change the CDC change event (any operation type: PRE_IMAGE, POST_IMAGE, delta)
   * @return a RowKey uniquely identifying this row within the task
   */
  public static RowKey from(TaskId taskId, RawChange change) {
    List<Object> pkValues = new ArrayList<>();
    for (ChangeSchema.ColumnDefinition cdef : change.getSchema().getNonCdcColumnDefinitions()) {
      ChangeSchema.ColumnType colType = cdef.getBaseTableColumnType();
      if (colType == ChangeSchema.ColumnType.PARTITION_KEY
          || colType == ChangeSchema.ColumnType.CLUSTERING_KEY) {
        pkValues.add(change.getCell(cdef.getColumnName()).getAsObject());
      }
    }
    return new RowKey(taskId, pkValues);
  }

  public TaskId getTaskId() {
    return taskId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RowKey)) return false;
    RowKey rowKey = (RowKey) o;
    return taskId.equals(rowKey.taskId) && primaryKeyValues.equals(rowKey.primaryKeyValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, primaryKeyValues);
  }

  @Override
  public String toString() {
    return "RowKey{taskId=" + taskId + ", pk=" + primaryKeyValues + "}";
  }
}
