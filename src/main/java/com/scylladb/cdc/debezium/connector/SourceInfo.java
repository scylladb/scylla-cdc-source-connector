package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.model.TableName;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.worker.TaskState;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.util.Collect;
import java.time.Instant;
import java.util.*;

public class SourceInfo extends BaseSourceInfo {

  public static final String KEYSPACE_NAME = "keyspace_name";
  public static final String TABLE_NAME = "table_name";
  public static final String VNODE_ID = "vnode_id";
  public static final String GENERATION_START = "generation_start";

  public static final String WINDOW_START = "window_start";
  public static final String WINDOW_END = "window_end";
  public static final String CHANGE_ID_STREAM_ID = "change_id_stream_id";
  public static final String CHANGE_ID_TIME = "change_id_time";

  private final TaskId taskId;
  private TaskState taskState;

  protected SourceInfo(ScyllaConnectorConfig connectorConfig, TaskId taskId) {
    super(connectorConfig);
    this.taskId = taskId;
  }

  public Map<String, String> partition() {
    return Collect.hashMapOf(
        KEYSPACE_NAME,
        taskId.getTable().keyspace,
        TABLE_NAME,
        taskId.getTable().name,
        VNODE_ID,
        Long.toString(taskId.getvNodeId().getIndex()),
        GENERATION_START,
        Long.toString(taskId.getGenerationId().getGenerationStart().toDate().getTime()));
  }

  public Map<String, String> offset() {
    Map<String, String> result =
        Collect.hashMapOf(
            WINDOW_START,
            taskState.getWindowStart().toString(),
            WINDOW_END,
            taskState.getWindowEnd().toString());
    taskState
        .getLastConsumedChangeId()
        .ifPresent(
            changeId -> {
              result.putAll(
                  Collect.hashMapOf(
                      CHANGE_ID_STREAM_ID,
                      Bytes.toHexString(changeId.getStreamId().getValue()),
                      CHANGE_ID_TIME,
                      changeId.getChangeTime().getUUID().toString()));
            });
    return result;
  }

  public TaskState getTaskState() {
    return taskState;
  }

  public void setTaskState(TaskState taskState) {
    this.taskState = taskState;
  }

  public void dataChangeEvent(TaskState taskState) {
    setTaskState(taskState);
  }

  public TableName getTableName() {
    return taskId.getTable();
  }

  @Override
  protected Instant timestamp() {
    return taskState.getLastConsumedChangeId().get().getChangeTime().getDate().toInstant();
  }

  public long timestampUs() {
    return taskState.getLastConsumedChangeId().get().getChangeTime().getTimestamp();
  }

  @Override
  protected String database() {
    // TODO - database() is required by Debezium. Currently returning the keyspace name.
    return getTableName().keyspace;
  }
}
