package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.VNodeId;
import java.util.Optional;

final class TabletTaskOffsetMigration {

  private TabletTaskOffsetMigration() {}

  static Optional<TaskId> legacyTaskId(TaskId taskId) {
    if (!taskId.isTabletStreamTask()) {
      return Optional.empty();
    }
    return Optional.of(new TaskId(taskId.getGenerationId(), new VNodeId(0), taskId.getTable()));
  }
}
