package com.scylladb.cdc.debezium.connector;

import com.google.common.collect.Lists;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class TaskConfigBuilder {
  private final Map<TaskId, SortedSet<StreamId>> tasks;

  public TaskConfigBuilder(Map<TaskId, SortedSet<StreamId>> tasks) {
    this.tasks = tasks;
  }

  public List<String> buildTaskConfigs(int maxTasks) {
    if (tasks == null) {
      return Collections.emptyList();
    }

    List<String> serializedTasks =
        tasks.entrySet().stream()
            .map(
                t -> {
                  TaskId taskId = t.getKey();
                  SortedSet<StreamId> streamIds = t.getValue();
                  return ConfigSerializerUtil.serializeTaskConfig(taskId, streamIds);
                })
            .collect(Collectors.toList());

    if (serializedTasks.isEmpty()) {
      return Collections.emptyList();
    }

    int partitionSize = (serializedTasks.size() + maxTasks - 1) / maxTasks;
    List<List<String>> partitionedTasks = Lists.partition(serializedTasks, partitionSize);

    List<String> taskConfigs =
        partitionedTasks.stream().map(q -> String.join("\n", q)).collect(Collectors.toList());
    assert taskConfigs.size() <= maxTasks;

    return taskConfigs;
  }
}
