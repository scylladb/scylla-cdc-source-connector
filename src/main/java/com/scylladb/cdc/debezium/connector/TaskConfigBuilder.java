package com.scylladb.cdc.debezium.connector;

import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;

public class TaskConfigBuilder {
  private static final class SerializedTask {
    private final String value;
    private final int sizeBytes;

    private SerializedTask(String value) {
      this.value = value;
      this.sizeBytes = value.getBytes(StandardCharsets.UTF_8).length;
    }
  }

  private static final class TaskConfigAccumulator {
    private final int index;
    private final List<SerializedTask> tasks = new ArrayList<>();
    private int sizeBytes;

    private TaskConfigAccumulator(int index) {
      this.index = index;
    }

    private boolean canFit(SerializedTask task, int maxBytes) {
      return (long) sizeBytes + (tasks.isEmpty() ? 0 : 1) + task.sizeBytes <= maxBytes;
    }

    private void add(SerializedTask task) {
      if (!tasks.isEmpty()) {
        sizeBytes++;
      }
      tasks.add(task);
      sizeBytes += task.sizeBytes;
    }

    private SerializedTask removeSmallest() {
      SerializedTask task = tasks.remove(tasks.size() - 1);
      sizeBytes -= task.sizeBytes;
      if (!tasks.isEmpty()) {
        sizeBytes--;
      }
      return task;
    }

    private String serialize() {
      return tasks.stream().map(task -> task.value).collect(Collectors.joining("\n"));
    }
  }

  private final Map<TaskId, SortedSet<StreamId>> tasks;
  private final int maxWorkerConfigBytes;

  public TaskConfigBuilder(Map<TaskId, SortedSet<StreamId>> tasks) {
    this(tasks, ScyllaConnectorConfig.DEFAULT_MAX_WORKER_CONFIG_BYTES);
  }

  TaskConfigBuilder(Map<TaskId, SortedSet<StreamId>> tasks, int maxWorkerConfigBytes) {
    if (maxWorkerConfigBytes <= 0) {
      throw new IllegalArgumentException("Maximum worker configuration size must be positive");
    }
    this.tasks = tasks;
    this.maxWorkerConfigBytes = maxWorkerConfigBytes;
  }

  public List<String> buildTaskConfigs(int maxTasks) {
    if (maxTasks <= 0) {
      throw new IllegalArgumentException("Maximum task count must be positive");
    }
    if (tasks == null) {
      return Collections.emptyList();
    }

    List<SerializedTask> serializedTasks =
        tasks.entrySet().stream()
            .map(
                t -> {
                  TaskId taskId = t.getKey();
                  SortedSet<StreamId> streamIds = t.getValue();
                  return new SerializedTask(
                      ConfigSerializerUtil.serializeTaskConfig(taskId, streamIds));
                })
            .collect(Collectors.toList());

    if (serializedTasks.isEmpty()) {
      return Collections.emptyList();
    }

    int taskConfigCount = Math.min(serializedTasks.size(), maxTasks);
    serializedTasks.sort(
        Comparator.comparingInt((SerializedTask task) -> task.sizeBytes)
            .reversed()
            .thenComparing(task -> task.value));

    // Least-loaded placement gives predictable parallelism and balanced task sizes. If its result
    // crosses the byte ceiling, retry with cap-aware best-fit packing before rejecting it.
    List<TaskConfigAccumulator> taskConfigAccumulators =
        packByLeastLoaded(serializedTasks, taskConfigCount);
    if (taskConfigAccumulators.stream()
        .anyMatch(config -> config.sizeBytes > maxWorkerConfigBytes)) {
      taskConfigAccumulators = packByBestFit(serializedTasks, taskConfigCount);
      if (taskConfigAccumulators == null) {
        throw oversizedConfiguration(taskConfigCount);
      }
    }

    return taskConfigAccumulators.stream()
        .map(TaskConfigAccumulator::serialize)
        .collect(Collectors.toList());
  }

  private static List<TaskConfigAccumulator> packByLeastLoaded(
      List<SerializedTask> serializedTasks, int taskConfigCount) {
    List<TaskConfigAccumulator> accumulators = new ArrayList<>(taskConfigCount);
    PriorityQueue<TaskConfigAccumulator> leastLoaded =
        new PriorityQueue<>(
            Comparator.comparingInt((TaskConfigAccumulator config) -> config.sizeBytes)
                .thenComparingInt(config -> config.tasks.size())
                .thenComparingInt(config -> config.index));
    for (int i = 0; i < taskConfigCount; i++) {
      TaskConfigAccumulator accumulator = new TaskConfigAccumulator(i);
      accumulators.add(accumulator);
      leastLoaded.add(accumulator);
    }
    for (SerializedTask serializedTask : serializedTasks) {
      TaskConfigAccumulator accumulator = leastLoaded.remove();
      accumulator.add(serializedTask);
      leastLoaded.add(accumulator);
    }
    return accumulators;
  }

  private List<TaskConfigAccumulator> packByBestFit(
      List<SerializedTask> serializedTasks, int taskConfigCount) {
    List<TaskConfigAccumulator> taskConfigAccumulators = new ArrayList<>(taskConfigCount);
    for (SerializedTask serializedTask : serializedTasks) {
      TaskConfigAccumulator accumulator =
          taskConfigAccumulators.stream()
              .filter(config -> config.canFit(serializedTask, maxWorkerConfigBytes))
              .max(
                  Comparator.comparingInt((TaskConfigAccumulator config) -> config.sizeBytes)
                      .thenComparingInt(config -> -config.index))
              .orElse(null);
      if (accumulator == null) {
        if (taskConfigAccumulators.size() >= taskConfigCount
            || serializedTask.sizeBytes > maxWorkerConfigBytes) {
          return null;
        }
        accumulator = new TaskConfigAccumulator(taskConfigAccumulators.size());
        taskConfigAccumulators.add(accumulator);
      }
      accumulator.add(serializedTask);
    }

    // Best-fit packing can use fewer records than Kafka Connect made available. Split packed
    // records without changing their byte safety so taskConfigs() returns exactly min(S, maxTasks).
    while (taskConfigAccumulators.size() < taskConfigCount) {
      TaskConfigAccumulator donor =
          taskConfigAccumulators.stream()
              .filter(config -> config.tasks.size() > 1)
              .max(
                  Comparator.comparingInt((TaskConfigAccumulator config) -> config.tasks.size())
                      .thenComparingInt(config -> config.sizeBytes)
                      .thenComparingInt(config -> -config.index))
              .orElseThrow(IllegalStateException::new);
      TaskConfigAccumulator split = new TaskConfigAccumulator(taskConfigAccumulators.size());
      split.add(donor.removeSmallest());
      taskConfigAccumulators.add(split);
    }

    return taskConfigAccumulators;
  }

  private ConnectException oversizedConfiguration(int availableTaskConfigs) {
    return new ConnectException(
        String.format(
            "Unable to safely pack the stream assignments into %d Kafka Connect task "
                + "configurations without exceeding scylla.worker.config.max.bytes=%,d. Increase "
                + "tasks.max or split the configured tables across connectors. Raise the byte "
                + "limit only after increasing Kafka's producer and config-topic record limits.",
            availableTaskConfigs, maxWorkerConfigBytes));
  }
}
